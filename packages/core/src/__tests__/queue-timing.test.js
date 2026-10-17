import { test, describe, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  Database,
  runMigrations,
  PaymentSessionRepository,
  ReceivingAccountRepository,
  AuditLogRepository
} from '@youpi/db';
import {
  QueueEngine,
  TimingEngine,
  RecoveryEngine
} from '../index.js';
import { PaymentStatus } from '@youpi/common';

describe('@youpi/core Queue & Timing Engine Tests', () => {
  let db;
  let sessionRepo;
  let accountRepo;
  let auditRepo;
  let queueEngine;
  let timingEngine;
  let recoveryEngine;
  let testAccount;
  let defaultMerchant;
  let events;

  beforeEach(() => {
    db = new Database({ inMemory: true });
    const seed = runMigrations(db);
    defaultMerchant = seed.defaultMerchant;
    testAccount = { id: '00000000-0000-0000-0000-000000000002' };

    sessionRepo = new PaymentSessionRepository(db);
    accountRepo = new ReceivingAccountRepository(db);
    auditRepo = new AuditLogRepository(db);

    events = [];
    const eventEmitter = {
      emit: (event, payload) => events.push({ event, payload })
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
      activeDurationMs: 100, // fast 100ms timer for testing
      processingDurationMs: 50 // fast 50ms timer for testing
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

  afterEach(() => {
    timingEngine.destroy();
    db.close();
  });

  test('Queue Engine correctly assigns active slot and queued positions with wait estimates', () => {
    const s1 = sessionRepo.create({
      merchantId: defaultMerchant.id,
      receivingAccountId: testAccount.id,
      orderId: 'Q-ORD-1',
      amountPaise: 20000n
    });
    const s2 = sessionRepo.create({
      merchantId: defaultMerchant.id,
      receivingAccountId: testAccount.id,
      orderId: 'Q-ORD-2',
      amountPaise: 20000n
    });
    const s3 = sessionRepo.create({
      merchantId: defaultMerchant.id,
      receivingAccountId: testAccount.id,
      orderId: 'Q-ORD-3',
      amountPaise: 20000n
    });

    // 1. Session 1 enters
    const res1 = queueEngine.enqueueOrActivate(s1.id, testAccount.id, 300000);
    assert.equal(res1.status, PaymentStatus.ACTIVE);
    assert.equal(res1.queuePosition, 0);

    // 2. Session 2 enters while the account is busy -> ACTIVE on its own unique amount
    const res2 = queueEngine.enqueueOrActivate(s2.id, testAccount.id, 300000);
    assert.equal(res2.status, PaymentStatus.ACTIVE);
    assert.equal(res2.queuePosition, 0);
    assert.equal(res2.amountAdjusted, true);

    // 3. Session 3 enters -> also ACTIVE, on a third distinct amount
    const res3 = queueEngine.enqueueOrActivate(s3.id, testAccount.id, 300000);
    assert.equal(res3.status, PaymentStatus.ACTIVE);
    assert.equal(res3.queuePosition, 0);

    // The invariant that makes concurrency safe: no two live sessions share an
    // amount, so an incoming credit always matches exactly one session.
    const liveAmounts = [s1, s2, s3].map((s) => Number(sessionRepo.findById(s.id).amountPaise));
    assert.equal(new Set(liveAmounts).size, 3);
    assert.equal(liveAmounts[0], 20000);
  });

  test('Timing Engine: 5-minute active window expires and promotes the next queued session', async () => {
    const s1 = sessionRepo.create({
      merchantId: defaultMerchant.id,
      receivingAccountId: testAccount.id,
      orderId: 'TIM-ORD-1',
      amountPaise: 20000n
    });
    const s2 = sessionRepo.create({
      merchantId: defaultMerchant.id,
      receivingAccountId: testAccount.id,
      orderId: 'TIM-ORD-2',
      amountPaise: 20000n
    });

    // Occupy the slot, then queue s2 through the raw primitive. The engine itself
    // only queues when every candidate amount is already taken.
    const res1 = sessionRepo.acquireSlotAtomic(s1.id, testAccount.id, 50); // expires in 50ms
    const queued = sessionRepo.acquireSlotAtomic(s2.id, testAccount.id, 50);
    assert.equal(queued.status, PaymentStatus.QUEUED);

    timingEngine.startActiveTimer(s1.id, res1.expiresAt);

    // Wait 70ms for s1 to expire
    await new Promise(resolve => setTimeout(resolve, 70));

    // s1 must now be EXPIRED
    const updatedS1 = sessionRepo.findById(s1.id);
    assert.equal(updatedS1.status, PaymentStatus.EXPIRED);

    // s2 must now be promoted to ACTIVE!
    const updatedS2 = sessionRepo.findById(s2.id);
    assert.equal(updatedS2.status, PaymentStatus.ACTIVE);
    assert.equal(updatedS2.queuePosition, 0);
  });

  test('Timing Engine: Payment Done starts 30-sec processing window, fails if no observation', async () => {
    const s1 = sessionRepo.create({
      merchantId: defaultMerchant.id,
      receivingAccountId: testAccount.id,
      orderId: 'PROC-ORD-1',
      amountPaise: 20000n
    });

    queueEngine.enqueueOrActivate(s1.id, testAccount.id, 300000);

    // Customer clicks Payment Done
    const procSession = timingEngine.handlePaymentDone(s1.id);
    assert.equal(procSession.status, PaymentStatus.PROCESSING);
    assert.ok(procSession.processingExpiresAt);

    // Wait 70ms for processing timer (configured as 50ms) to elapse
    await new Promise(resolve => setTimeout(resolve, 70));

    // s1 must now be FAILED because no observer verified it
    const updatedS1 = sessionRepo.findById(s1.id);
    assert.equal(updatedS1.status, PaymentStatus.FAILED);
  });

  test('Recovery Engine: crash recovery cleans up overdue sessions and promotes queued', () => {
    // Simulate a crash scenario:
    // Session 1 was ACTIVE but expires_at was 10 minutes ago
    const pastDate = new Date(Date.now() - 600000).toISOString();
    const s1 = sessionRepo.create({
      merchantId: defaultMerchant.id,
      receivingAccountId: testAccount.id,
      orderId: 'CRASH-ORD-1',
      amountPaise: 20000n
    });
    db.execute(`
      UPDATE payment_sessions
      SET status = ?, expires_at = ?, slot_acquired_at = ?
      WHERE id = ?
    `, [PaymentStatus.ACTIVE, pastDate, pastDate, s1.id]);
    db.execute(`
      UPDATE payment_receiving_accounts
      SET is_slot_busy = 1, current_active_session_id = ?
      WHERE id = ?
    `, [s1.id, testAccount.id]);

    // Session 2 was QUEUED waiting for slot
    const s2 = sessionRepo.create({
      merchantId: defaultMerchant.id,
      receivingAccountId: testAccount.id,
      orderId: 'CRASH-ORD-2',
      amountPaise: 20000n,
      status: PaymentStatus.QUEUED,
      queuePosition: 1
    });

    // Run crash recovery
    const stats = recoveryEngine.recover();
    assert.equal(stats.expiredCount, 1);
    assert.equal(stats.promotedCount, 1);

    // S1 must be EXPIRED
    const checkS1 = sessionRepo.findById(s1.id);
    assert.equal(checkS1.status, PaymentStatus.EXPIRED);

    // S2 must be promoted to ACTIVE
    const checkS2 = sessionRepo.findById(s2.id);
    assert.equal(checkS2.status, PaymentStatus.ACTIVE);
    assert.equal(checkS2.queuePosition, 0);
  });
});
