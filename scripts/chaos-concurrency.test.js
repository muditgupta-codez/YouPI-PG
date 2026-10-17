import { test, describe, before, after } from 'node:test';
import assert from 'node:assert/strict';
import {
  Database,
  runMigrations,
  PaymentSessionRepository,
  ReceivingAccountRepository,
  ObservationRepository,
  SupportTicketRepository,
  AuditLogRepository,
  CustomerRepository
} from '@youpi/db';
import {
  QueueEngine,
  TimingEngine,
  AttributionEngine,
  RecoveryEngine,
  ObservationNormalizer
} from '@youpi/core';
import { PaymentStatus, ObserverSource, ObservationAttributionStatus } from '@youpi/common';

describe('Phase 16: Comprehensive Chaos & Concurrency Verification', () => {
  let db;
  let sessionRepo;
  let accountRepo;
  let obsRepo;
  let ticketRepo;
  let auditRepo;
  let customerRepo;
  let queueEngine;
  let timingEngine;
  let attributionEngine;
  let recoveryEngine;
  let testAccount;
  let defaultMerchant;
  let emittedEvents;
  // Shared across the CHAOS narrative: the ten sessions created in CHAOS 1.
  const sessionIds = [];

  before(() => {
    db = new Database({ inMemory: true });
    const seed = runMigrations(db);
    defaultMerchant = seed.defaultMerchant;
    testAccount = { id: '00000000-0000-0000-0000-000000000002' };

    sessionRepo = new PaymentSessionRepository(db);
    accountRepo = new ReceivingAccountRepository(db);
    obsRepo = new ObservationRepository(db);
    ticketRepo = new SupportTicketRepository(db);
    auditRepo = new AuditLogRepository(db);
    customerRepo = new CustomerRepository(db);

    emittedEvents = [];
    const eventEmitter = {
      emit: (event, payload) => emittedEvents.push({ event, payload })
    };

    queueEngine = new QueueEngine({
      sessionRepo,
      accountRepo,
      eventEmitter,
      auditRepo
    });

    timingEngine = new TimingEngine({
      sessionRepo,
      queueEngine,
      eventEmitter,
      auditRepo,
      activeDurationMs: 1000,
      processingDurationMs: 500
    });

    attributionEngine = new AttributionEngine({
      sessionRepo,
      obsRepo,
      ticketRepo,
      auditRepo,
      eventEmitter
    });

    recoveryEngine = new RecoveryEngine({
      db,
      sessionRepo,
      accountRepo,
      queueEngine,
      timingEngine,
      auditRepo
    });
  });

  after(() => {
    timingEngine.destroy();
    db.close();
  });

  test('CHAOS 1: 10 simultaneous sessions for identical amounts (₹200) activate concurrently on distinct payable amounts', () => {
    // Create 10 sessions for identical ₹200.00
    for (let i = 1; i <= 10; i++) {
      const s = sessionRepo.create({
        merchantId: defaultMerchant.id,
        receivingAccountId: testAccount.id,
        orderId: `CHAOS-ORD-${i}`,
        amountPaise: 20000n
      });
      sessionIds.push(s.id);
    }

    // Simultaneously attempt activation — nobody is made to wait for a slot
    const results = sessionIds.map(id => queueEngine.enqueueOrActivate(id, testAccount.id, 300000));

    // Every payer goes ACTIVE immediately: concurrency is absorbed by amount, not by queueing
    assert.equal(results.filter(r => r.status === PaymentStatus.ACTIVE).length, 10);
    assert.equal(results.filter(r => r.status === PaymentStatus.QUEUED).length, 0);
    assert.ok(results.every(r => r.queuePosition === 0));

    // THE INVARIANT: each live session owns a payable amount nobody else holds. This is the
    // only key a bank credit can be matched on, since SMS/email carry no session identifier.
    const amounts = sessionIds.map(id => Number(sessionRepo.findById(id).amountPaise));
    assert.equal(new Set(amounts).size, 10);
    assert.equal(amounts[0], 20000); // the first payer pays exactly what was asked for
    assert.deepEqual(
      amounts.slice().sort((a, b) => a - b),
      Array.from({ length: 10 }, (_, i) => 20000 + i)
    );

    // Only the first payer kept the requested amount; the other nine were nudged by paise
    assert.equal(results.filter(r => r.amountAdjusted === true).length, 9);

    // All ten share the one receiving account that owns the slot
    const activeAccount = accountRepo.findById(testAccount.id);
    assert.equal(activeAccount.isSlotBusy, true);
    assert.ok(sessionIds.includes(activeAccount.currentActiveSessionId));
  });

  test('CHAOS 2: Multi-tab simultaneous Payment Done clicks are strictly idempotent', () => {
    const activeSession = sessionRepo.findById(sessionIds[0]);
    assert.equal(activeSession.status, PaymentStatus.ACTIVE);

    // Simulate 5 browser tabs clicking Payment Done at the exact same instant
    const results = [1, 2, 3, 4, 5].map(() => {
      try {
        return timingEngine.handlePaymentDone(activeSession.id);
      } catch (err) {
        return { error: err.message };
      }
    });

    // All must resolve to status PROCESSING
    for (const r of results) {
      assert.equal(r.status, PaymentStatus.PROCESSING);
    }

    // Session in DB must be PROCESSING
    const checkDb = sessionRepo.findById(activeSession.id);
    assert.equal(checkDb.status, PaymentStatus.PROCESSING);
  });

  test('CHAOS 3: Dual-channel flood (Gmail + SMS) guarantees exactly 1 payment attribution', () => {
    // Targeted by id, not by findCurrentActiveOrProcessing: with concurrent payers that
    // helper returns an arbitrary live session, so it can no longer identify "the" one.
    const activeSession = sessionRepo.findById(sessionIds[0]);
    assert.equal(activeSession.status, PaymentStatus.PROCESSING);

    // Simulate SMS and Gmail seeing the exact same ₹200 transfer (same UTR)
    const utr = 'UTR-CHAOS-9999';

    const obsSms = ObservationNormalizer.normalize({
      source: ObserverSource.GOOGLE_MESSAGES,
      receivingAccountId: testAccount.id,
      amountPaise: 20000n,
      bankReferenceNumber: utr,
      rawPayload: { text: 'SMS Rs 200' }
    });

    const obsGmail = ObservationNormalizer.normalize({
      source: ObserverSource.GMAIL,
      receivingAccountId: testAccount.id,
      amountPaise: 20000n,
      bankReferenceNumber: utr,
      rawPayload: { text: 'Email Rs 200' }
    });

    const resSms = attributionEngine.processObservation(obsSms);
    const resGmail = attributionEngine.processObservation(obsGmail);

    // Multi-channel flood confirms payment at Level 1, then upgrades to Level 2 (Dual Verification)!
    assert.equal(resSms.attributed, true);
    assert.equal(resSms.outcomeStatus, PaymentStatus.SUCCESS);
    assert.equal(resSms.verificationLevel, 1);

    assert.equal(resGmail.attributed, true);
    assert.equal(resGmail.verificationLevel, 2);

    // Redundant duplicate from the same source (SMS again) is safely ignored!
    const resSmsDup = attributionEngine.processObservation(obsSms);
    assert.equal(resSmsDup.attributed, false);
    assert.equal(resSmsDup.duplicate, true);
    assert.equal(resSmsDup.outcomeStatus, ObservationAttributionStatus.DUPLICATE_IGNORED);

    // Session transitions to SUCCESS
    const checkDb = sessionRepo.findById(activeSession.id);
    assert.equal(checkDb.status, PaymentStatus.SUCCESS);
  });

  test('CHAOS 4: Settling one concurrent session leaves its peers live and independently attributable', () => {
    // CHAOS 3 settled sessionIds[0]; its nine peers must be completely undisturbed
    const survivors = sessionIds.slice(1).map(id => sessionRepo.findById(id));
    assert.equal(survivors.filter(s => s.status === PaymentStatus.ACTIVE).length, 9);

    // A credit must land on the one live session holding that exact amount, never on
    // whichever session merely happens to be "current" on the account.
    const target = survivors[1]; // sessionIds[2] — distinct amount, e.g. ₹200.02

    const obs = ObservationNormalizer.normalize({
      source: ObserverSource.GMAIL,
      receivingAccountId: testAccount.id,
      amountPaise: target.amountPaise,
      bankReferenceNumber: 'UTR-CHAOS-CONCURRENT-2',
      rawPayload: { text: 'Email Rs 200.02' }
    });

    const res = attributionEngine.processObservation(obs);
    assert.equal(res.attributed, true);
    assert.equal(res.paymentSessionId, target.id);
    assert.equal(res.outcomeStatus, PaymentStatus.SUCCESS);
    assert.equal(sessionRepo.findById(target.id).status, PaymentStatus.SUCCESS);

    // The eight bystanders are still waiting for their own credits
    const stillActive = survivors
      .filter(s => s.id !== target.id)
      .map(s => sessionRepo.findById(s.id))
      .filter(s => s.status === PaymentStatus.ACTIVE);
    assert.equal(stillActive.length, 8);

    // Nothing was ever queued, so there is no promotion cascade left to run
    assert.equal(sessionRepo.findQueuedSessions(testAccount.id).length, 0);
  });

  test('CHAOS 5: Late observation does not hijack a live customer session', () => {
    // Any one of the still-live sessions stands in for customer B
    const activeCustomerB = sessionIds[1];
    assert.equal(sessionRepo.findById(activeCustomerB).status, PaymentStatus.ACTIVE);

    // A delayed observation arrives for an earlier customer session that expired
    const expiredCust = customerRepo.create({
      merchantId: defaultMerchant.id,
      phoneNumber: '9000011111',
      fullName: 'Late Customer',
      password: 'Password1!'
    });

    // A unique amount, so the late match cannot collide with any settled/live session
    const expiredSession = sessionRepo.create({
      merchantId: defaultMerchant.id,
      customerId: expiredCust.id,
      receivingAccountId: testAccount.id,
      orderId: 'ORD-LATE-CUSTOMER',
      amountPaise: 30000n
    });
    sessionRepo.acquireSlotAtomic(expiredSession.id, testAccount.id, 100);
    sessionRepo.transitionToTerminal(expiredSession.id, PaymentStatus.EXPIRED);

    // Observation arrives matching the expired amount (30000n)
    const delayedObs = ObservationNormalizer.normalize({
      source: ObserverSource.GMAIL,
      receivingAccountId: testAccount.id,
      amountPaise: 30000n,
      bankReferenceNumber: 'UTR-DELAYED-3322',
      observedAt: new Date(Date.now() - 60000)
    });

    const lateResult = attributionEngine.processObservation(delayedObs);

    // MUST NOT attribute to Active Customer B!
    assert.equal(lateResult.attributed, false);
    assert.equal(lateResult.lateMatch, true);
    assert.equal(lateResult.paymentSessionId, expiredSession.id);

    // Customer B must still be ACTIVE
    const checkB = sessionRepo.findById(activeCustomerB);
    assert.equal(checkB.status, PaymentStatus.ACTIVE);
  });
});
