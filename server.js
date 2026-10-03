// ============================================
// RC RECORDS SERVER — v6.5 (SHOWS + TICKETS + CHECK-IN)
// Base: v6.4 + ticket_id + venue + gate check-in
// ============================================

require('dotenv').config();
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const cors = require('cors');
const helmet = require('helmet');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const sqlite3 = require('sqlite3').verbose();
const { ethers } = require('ethers');

const SYSTEM_ID = process.env.SYSTEM_ID || 'desktop';
console.log(`🖥️ Starting ${SYSTEM_ID} server (Node v${process.version})...`);

const PORT = process.env.PORT || 3000;
const FALLBACK_SERVER_URL = process.env.FALLBACK_SERVER_URL || 'https://dz9lgzzvaqug-production-nc6hkzmr.europe-west1.suga.run';
const IS_CLOUD = process.env.IS_CLOUD === 'true' || SYSTEM_ID === 'suga-fallback';
const BSC_RPC_URL = process.env.BSC_RPC_URL || 'https://data-seed-prebsc-1-s1.binance.org:8545/';
const BSC_CONTRACT_ADDRESS = process.env.BSC_CONTRACT_ADDRESS || '0x4d1f190750b0c2ca61d79acd0e9669eae9e7554b';
const BSC_PRIVATE_KEY = process.env.BSC_PRIVATE_KEY || '';
const CORS_ORIGIN = process.env.CORS_ORIGIN || '*';
const PACKET_SECRET = process.env.PACKET_SIGNING_SECRET || 'dev-secret-change-me';
const RATE_LIMIT = parseInt(process.env.RATE_LIMIT_PACKETS_PER_MIN) || 120;
const RATE_WINDOW_MS = 60_000;
const HEARTBEAT_RETRY_MAX = parseInt(process.env.HEARTBEAT_RETRY_MAX) || 5;
const ADMIN_CRYPTO_WALLET = process.env.ADMIN_CRYPTO_WALLET || null;
const ADMIN_CRYPTO_NETWORK = process.env.ADMIN_CRYPTO_NETWORK || null;

console.log(`   Role: ${IS_CLOUD ? '☁️  CLOUD FALLBACK' : '🖥️  DESKTOP PRIMARY'}`);

// ============================================
// SYSTEM WALLETS & CONSTANTS
// ============================================
const WALLET_IDS = {
    SYSTEM: 'RC-SYS000',
    ADMIN: 'RC-ADM456',
    CROWN_BANK: 'RC-CRN456',
    CASH_BOX: 'RC-CBX564',
    ISSUANCE_AUTHORITY: 'RC-ISSUER'
};
const ADMIN_WALLETS = [WALLET_IDS.ADMIN, 'ADMIN', 'ADMIN_VAULT'];
const VALID_TOKENS = ['RCT', 'RGT', 'IRT', 'RCASH', 'LGT', 'EMP', 'TAX', 'ET', 'RT'];

// ============================================
// DATABASE
// ============================================
const DB_FILE = process.env.DB_PATH || `./data_${SYSTEM_ID}.db`;
const db = new sqlite3.Database(DB_FILE);

db.run('PRAGMA journal_mode = WAL');
db.run('PRAGMA synchronous = NORMAL');
db.run('PRAGMA foreign_keys = ON');
db.run('PRAGMA busy_timeout = 5000');

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS members (
        wallet TEXT PRIMARY KEY, eth TEXT, name TEXT, username TEXT,
        email TEXT, phone TEXT, address TEXT, role TEXT, tier INTEGER,
        amount_paid REAL, token_balance REAL DEFAULT 0,
        registered_at INTEGER, expiry_date TEXT,
        status TEXT DEFAULT 'active', client_secret TEXT,
        crypto_wallet TEXT, crypto_network TEXT
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS balances (
        wallet TEXT, token TEXT, amount REAL DEFAULT 0,
        PRIMARY KEY (wallet, token)
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tx_id TEXT UNIQUE NOT NULL, type TEXT NOT NULL,
        from_wallet TEXT, to_wallet TEXT, amount REAL, token TEXT,
        timestamp INTEGER, status TEXT DEFAULT 'confirmed',
        extra TEXT, sync_status TEXT DEFAULT 'pending_sync',
        instruction TEXT,
        packet_signature TEXT,
        packet_id TEXT
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS nfts (
        id TEXT PRIMARY KEY, artist_name TEXT, artist_wallet TEXT,
        total_shares REAL, shares_available REAL, price_per_share REAL,
        token TEXT, slot TEXT, monthly_return REAL, share_per_unit REAL,
        image_url TEXT, description TEXT, benefits TEXT,
        status TEXT DEFAULT 'active', minted_by TEXT, minted_at INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS feed_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_wallet TEXT, message TEXT, image TEXT,
        category TEXT DEFAULT 'Client', timestamp INTEGER,
        attendCount INTEGER DEFAULT 0, wantCount INTEGER DEFAULT 0
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_wallet TEXT, to_wallet TEXT, body TEXT, image TEXT,
        timestamp INTEGER, read INTEGER DEFAULT 0
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS broadcasts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_wallet TEXT, body TEXT, image TEXT, timestamp INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS vouchers (
        code TEXT PRIMARY KEY, amount REAL, token TEXT,
        expires_at INTEGER, max_uses INTEGER DEFAULT 1,
        used_count INTEGER DEFAULT 0, created_by TEXT,
        created_at INTEGER, active INTEGER DEFAULT 1, to_wallet TEXT
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS pending_funding (
        wallet TEXT PRIMARY KEY, name TEXT, username TEXT, role TEXT,
        tier INTEGER, amount_paid REAL, token_amount REAL, exchange_rate REAL,
        status TEXT DEFAULT 'pending', created_at INTEGER, admin TEXT
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS pending_cashouts (
        id TEXT PRIMARY KEY, wallet TEXT, amount REAL, currency TEXT,
        bankDetails TEXT, status TEXT DEFAULT 'pending',
        created_at INTEGER, approved_by TEXT, approved_at INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS penalty_vault (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet TEXT, amount REAL, reason TEXT, date INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS sync_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tx_id TEXT, data TEXT, attempts INTEGER DEFAULT 0,
        created_at INTEGER, status TEXT DEFAULT 'pending'
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS notes (
        id TEXT PRIMARY KEY, wallet TEXT, title TEXT, content TEXT,
        created_at INTEGER, updated_at INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS heartbeat_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tx_hash TEXT, state_hash TEXT, wallet_count INTEGER,
        timestamp INTEGER, status TEXT
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS pending_registrations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT, username TEXT, email TEXT, phone TEXT, address TEXT,
        role TEXT, tier INTEGER, voucher TEXT, extra_services TEXT,
        submitted_at INTEGER, status TEXT DEFAULT 'pending'
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS creative_works (
        id TEXT PRIMARY KEY, catalog_id TEXT UNIQUE, work_type TEXT NOT NULL,
        title TEXT NOT NULL, creator_wallet TEXT NOT NULL, creator_name TEXT,
        co_creators TEXT, description TEXT, genre TEXT, language TEXT,
        duration INTEGER, pages INTEGER, release_date TEXT,
        isrc TEXT, isbn TEXT, imdb_id TEXT, script_id TEXT,
        file_hash TEXT, file_url TEXT, cover_url TEXT,
        status TEXT DEFAULT 'registered', registered_by TEXT, registered_at INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS merch_registry (
        id TEXT PRIMARY KEY, catalog_id TEXT UNIQUE, merch_type TEXT NOT NULL,
        title TEXT NOT NULL, creator_wallet TEXT NOT NULL, creator_name TEXT,
        description TEXT, category TEXT, linked_work_id TEXT,
        price REAL, token TEXT DEFAULT 'RGT', stock INTEGER DEFAULT 0,
        sizes TEXT, colors TEXT, materials TEXT, sku TEXT,
        image_url TEXT, image_urls TEXT,
        status TEXT DEFAULT 'registered', registered_by TEXT, registered_at INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS purchase_quotes (
        quote_id TEXT PRIMARY KEY, cash_request_id TEXT, provider TEXT,
        network TEXT, asset TEXT, amount_ngn REAL, destination TEXT,
        rate REAL, fee_ngn REAL, receive_amount REAL,
        expires_at TEXT, status TEXT DEFAULT 'open',
        created_at INTEGER, wallet TEXT
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS tax_withholding (
        id TEXT PRIMARY KEY, cash_request_id TEXT, quote_id TEXT,
        wallet TEXT, amount_ngn REAL, amount_token REAL, asset TEXT,
        status TEXT DEFAULT 'withheld', created_at INTEGER, remitted_at INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS issuance_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token TEXT NOT NULL, amount REAL NOT NULL, reason TEXT NOT NULL,
        beneficiary TEXT NOT NULL, ref_tx_id TEXT, metadata TEXT, timestamp INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS redemption_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token TEXT NOT NULL, amount REAL NOT NULL, reason TEXT NOT NULL,
        from_wallet TEXT NOT NULL, fiat_amount REAL, currency TEXT,
        ref_tx_id TEXT, timestamp INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS bridge_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token TEXT NOT NULL, amount REAL NOT NULL, direction TEXT,
        from_wallet TEXT, to_wallet TEXT, to_chain TEXT, to_address TEXT,
        tx_hash TEXT, timestamp INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS processed_packets (
        packet_id TEXT PRIMARY KEY, wallet TEXT,
        processed_at INTEGER, result TEXT
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS applied_tx (
        tx_id TEXT PRIMARY KEY, applied_at INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS wallet_sessions (
        wallet TEXT PRIMARY KEY, public_key TEXT, secret_hash TEXT,
        created_at INTEGER, last_seen INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS heartbeat_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state_hash TEXT, wallet_count INTEGER, attempts INTEGER DEFAULT 0,
        created_at INTEGER, status TEXT DEFAULT 'pending', last_error TEXT
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS module_boxes (
        id TEXT PRIMARY KEY, code TEXT UNIQUE, name TEXT, wallet TEXT,
        source TEXT, token TEXT, slot TEXT, amount REAL DEFAULT 0,
        mode TEXT, window_ms INTEGER DEFAULT 0, status TEXT DEFAULT 'not-programmed',
        delivered REAL DEFAULT 0, total_delivered REAL DEFAULT 0, sends INTEGER DEFAULT 0,
        drip_started_at INTEGER, next_fire_at INTEGER, last_fire_at INTEGER,
        history TEXT, created_at INTEGER, updated_at INTEGER, created_by TEXT
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS cover_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cover_bg TEXT, updated_at INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS wallet_snapshots (
        wallet TEXT PRIMARY KEY, vault TEXT, crown TEXT,
        analytics TEXT, updated_at INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS exchange_rates (
        token TEXT PRIMARY KEY, rate_ngn REAL NOT NULL,
        currency TEXT DEFAULT 'NGN', updated_by TEXT, updated_at INTEGER
    )`);
    db.run(`CREATE TABLE IF NOT EXISTS settlement_slips (
        slip_id TEXT PRIMARY KEY, wallet TEXT NOT NULL,
        token TEXT NOT NULL, token_amount REAL NOT NULL,
        rate REAL NOT NULL, ngn_value REAL NOT NULL,
        bank_details TEXT, crypto_wallet TEXT, crypto_network TEXT,
        status TEXT DEFAULT 'pending', quidax_tx_hash TEXT,
        quidax_reference TEXT, quidax_order_id TEXT,
        usdt_amount REAL, error TEXT,
        created_at INTEGER, completed_at INTEGER
    )`);

    db.run(`ALTER TABLE settlement_slips ADD COLUMN provider TEXT DEFAULT 'quidax'`, () => {});

    // ---- Shows + Attendance tables (v6.5 adds ticket_id + venue + check-in columns) ----
    db.run(`CREATE TABLE IF NOT EXISTS shows (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        artist_wallet TEXT NOT NULL,
        artist_name TEXT,
        scheduled_at INTEGER,
        duration_minutes INTEGER DEFAULT 60,
        ticket_price REAL DEFAULT 0,
        ticket_token TEXT DEFAULT 'RGT',
        attendee_reward REAL DEFAULT 0,
        reward_token TEXT DEFAULT 'RGT',
        artist_rate_per_min REAL DEFAULT 0.1,
        artist_stream_token TEXT DEFAULT 'RGT',
        listener_rate_per_min REAL DEFAULT 0.01,
        listener_stream_token TEXT DEFAULT 'RGT',
        poster_url TEXT,
        video_url TEXT,
        status TEXT DEFAULT 'scheduled',
        is_virtual INTEGER DEFAULT 1,
        venue TEXT,
        venue_address TEXT,
        check_in_opens_at INTEGER,
        created_by TEXT,
        created_at INTEGER,
        announced_at INTEGER,
        started_at INTEGER,
        ended_at INTEGER,
        attendee_count INTEGER DEFAULT 0,
        total_ticket_revenue REAL DEFAULT 0,
        total_reward_paid REAL DEFAULT 0,
        total_stream_paid REAL DEFAULT 0
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS show_attendance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        show_id TEXT NOT NULL,
        attendee_wallet TEXT NOT NULL,
        ticket_paid REAL DEFAULT 0,
        ticket_token TEXT DEFAULT 'RGT',
        reward_received REAL DEFAULT 0,
        reward_token TEXT DEFAULT 'RGT',
        stream_earned REAL DEFAULT 0,
        stream_token TEXT DEFAULT 'RGT',
        watched_seconds INTEGER DEFAULT 0,
        purchased_at INTEGER,
        watched_at INTEGER,
        last_tick_at INTEGER,
        status TEXT DEFAULT 'attending',
        ticket_id TEXT,
        checked_in_at INTEGER,
        checked_in_by TEXT,
        admission_status TEXT DEFAULT 'pending',
        UNIQUE(show_id, attendee_wallet)
    )`);

    console.log(`✅ Database ready: ${DB_FILE}`);
    console.log(`✅ All tables created (including shows + attendance)`);
    console.log(`✅ v6.5 neural chain + shows + tickets + check-in`);

    db.get('SELECT COUNT(*) as c FROM exchange_rates', (err, row) => {
        if (!err && row && row.c === 0) {
            const now = Date.now();
            db.run(`INSERT INTO exchange_rates (token, rate_ngn, currency, updated_at) VALUES 
                ('RGT', 15.50, 'NGN', ?),
                ('RCT', 12.00, 'NGN', ?),
                ('IRT', 8.75, 'NGN', ?),
                ('RCASH', 1.00, 'NGN', ?),
                ('LGT', 20.00, 'NGN', ?),
                ('EMP', 5.00, 'NGN', ?),
                ('TAX', 1.00, 'NGN', ?),
                ('ET', 3.00, 'NGN', ?),
                ('RT', 10.00, 'NGN', ?)`,
                [now, now, now, now, now, now, now, now, now]);
            console.log(`✅ Seeded 9 default exchange rates`);
        }
    });
});

// ============================================
// MIGRATION — safe ALTER TABLE for existing DBs
// ============================================
function dbGetColumns(table) {
    return new Promise((resolve) => {
        db.all(`PRAGMA table_info(${table})`, (err, rows) => {
            if (err || !rows) return resolve([]);
            resolve(rows.map(r => r.name));
        });
    });
}
async function ensureColumn(table, column, typeSql) {
    const cols = await dbGetColumns(table);
    if (cols.includes(column)) return false;
    await new Promise((resolve) => {
        db.run(`ALTER TABLE ${table} ADD COLUMN ${column} ${typeSql}`, (err) => {
            if (err) console.warn(`⚠️  Migration ${table}.${column}: ${err.message}`);
            else console.log(`✅ Migrated: ${table}.${column}`);
            resolve();
        });
    });
    return true;
}
async function runMigrations() {
    console.log('🔧 Running safe migrations...');
    // shows
    await ensureColumn('shows', 'venue', 'TEXT');
    await ensureColumn('shows', 'venue_address', 'TEXT');
    await ensureColumn('shows', 'check_in_opens_at', 'INTEGER');
    // show_attendance
    await ensureColumn('show_attendance', 'ticket_id', 'TEXT');
    await ensureColumn('show_attendance', 'checked_in_at', 'INTEGER');
    await ensureColumn('show_attendance', 'checked_in_by', 'TEXT');
    await ensureColumn('show_attendance', 'admission_status', "TEXT DEFAULT 'pending'");
    // backfill admission_status for old rows
    await new Promise((resolve) => {
        db.run(`UPDATE show_attendance SET admission_status = 'pending' WHERE admission_status IS NULL`, () => resolve());
    });
    console.log('✅ Migrations complete');
}

// ============================================
// DATABASE HELPERS
// ============================================
function dbGet(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.get(sql, params, (err, row) => err ? reject(err) : resolve(row));
    });
}
function dbAll(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.all(sql, params, (err, rows) => err ? reject(err) : resolve(rows));
    });
}
function dbRun(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.run(sql, params, function(err) { err ? reject(err) : resolve(this); });
    });
}

// ============================================
// TICKET ID HELPER
// ============================================
function generateTicketId(showId, wallet) {
    const showSuffix = String(showId || '').replace(/[^a-zA-Z0-9]/g, '').slice(-6) || 'SHOW00';
    const walletSafe = String(wallet || 'UNKNOWN').replace(/[^a-zA-Z0-9\-]/g, '');
    const random6 = Math.random().toString(36).substring(2, 8).padEnd(6, '0');
    return `TKT_${showSuffix}_${walletSafe}_${random6}`;
}

// ============================================
// RATE LIMITING
// ============================================
const rateBuckets = new Map();
function checkRateLimit(wallet) {
    if (!wallet || wallet === 'UNKNOWN') return { ok: true };
    const now = Date.now();
    let bucket = rateBuckets.get(wallet);
    if (!bucket) { bucket = { tokens: RATE_LIMIT, lastRefill: now }; rateBuckets.set(wallet, bucket); }
    const elapsed = now - bucket.lastRefill;
    if (elapsed > RATE_WINDOW_MS) { bucket.tokens = RATE_LIMIT; bucket.lastRefill = now; }
    else {
        const refill = Math.floor((elapsed / RATE_WINDOW_MS) * RATE_LIMIT);
        bucket.tokens = Math.min(RATE_LIMIT, bucket.tokens + refill);
        bucket.lastRefill = now;
    }
    if (bucket.tokens < 1) return { ok: false, error: 'Rate limit exceeded' };
    bucket.tokens -= 1;
    return { ok: true };
}
setInterval(() => {
    const now = Date.now();
    for (const [w, b] of rateBuckets) if (now - b.lastRefill > 10 * 60_000) rateBuckets.delete(w);
}, 5 * 60_000);

// ============================================
// PACKET SIGNATURE
// ============================================
function verifyPacketSignature(packet) {
    const skipTypes = ['REGISTRATION_REQUEST', 'HEARTBEAT'];
    if (skipTypes.includes(packet.type)) return { ok: true, skipped: true };
    if (!packet.sig) return { ok: true, unsigned: true };
    const { sig, from_wallet, type, timestamp } = packet;
    if (!from_wallet || !type || !timestamp) return { ok: false, error: 'Missing sig fields' };
    const payloadStr = JSON.stringify(packet.payload || packet.data || {});
    const message = `${from_wallet}|${type}|${timestamp}|${payloadStr}`;
    const expected = crypto.createHmac('sha256', PACKET_SECRET).update(message).digest('hex');
    let a, b;
    try { a = Buffer.from(sig, 'hex'); b = Buffer.from(expected, 'hex'); } catch (e) { return { ok: false, error: 'Malformed sig' }; }
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return { ok: false, error: 'Invalid sig' };
    if (Math.abs(Date.now() - timestamp) > 5 * 60_000) return { ok: false, error: 'Sig expired' };
    return { ok: true };
}

// ============================================
// IDEMPOTENCY
// ============================================
const seenPackets = new Map();
const PACKET_TTL = 5 * 60_000;
async function checkIdempotency(packet) {
    if (!packet.packet_id) return { ok: true, skipped: true };
    const now = Date.now();
    const mem = seenPackets.get(packet.packet_id);
    if (mem && now - mem < PACKET_TTL) return { ok: false, duplicate: true, error: 'Duplicate packet_id' };
    const row = await dbGet('SELECT packet_id FROM processed_packets WHERE packet_id = ?', [packet.packet_id]);
    if (row) return { ok: false, duplicate: true, error: 'Duplicate packet_id' };
    seenPackets.set(packet.packet_id, now);
    return { ok: true };
}
async function recordProcessedPacket(packet, result) {
    if (!packet.packet_id) return;
    try {
        await dbRun(
            `INSERT OR REPLACE INTO processed_packets (packet_id, wallet, processed_at, result) VALUES (?, ?, ?, ?)`,
            [packet.packet_id, packet.from_wallet || 'UNKNOWN', Date.now(), JSON.stringify(result).slice(0, 2000)]
        );
    } catch (e) {}
}

// ============================================
// FREEZE/BLACKLIST GUARD
// ============================================
async function guardActive(wallet) {
    if (!wallet) return { ok: false, error: 'Wallet required' };
    if (ADMIN_WALLETS.includes(wallet)) return { ok: true, admin: true };
    const row = await dbGet('SELECT status FROM members WHERE wallet = ?', [wallet]);
    if (!row) return { ok: true, unregistered: true };
    if (row.status === 'frozen') return { ok: false, error: 'Wallet frozen' };
    if (row.status === 'blacklisted') return { ok: false, error: 'Wallet blacklisted' };
    return { ok: true };
}

// ============================================
// VALIDATION
// ============================================
async function validatePacket(packet) {
    if (!packet || !packet.type) return { valid: false, error: 'type required' };
    const systemTypes = ['REGISTRATION_REQUEST', 'HEARTBEAT'];
    if (!systemTypes.includes(packet.type) && !packet.from_wallet) return { valid: false, error: 'from_wallet required' };
    if (packet.amount !== undefined && packet.amount !== null) {
        const amt = parseFloat(packet.amount);
        if (isNaN(amt) || amt < 0) return { valid: false, error: 'invalid amount' };
        if (amt > 1000000000) return { valid: false, error: 'amount too large' };
    }
    if (packet.token && packet.token !== 'AUTO' && packet.token !== 'NGN' && packet.token !== 'USD' && !VALID_TOKENS.includes(packet.token)) {
        return { valid: false, error: 'invalid token: ' + packet.token };
    }
    const sigCheck = verifyPacketSignature(packet);
    if (!sigCheck.ok) return { valid: false, error: sigCheck.error };
    return { valid: true };
}

// ============================================
// HELPERS
// ============================================
function isValidToken(token) { return VALID_TOKENS.includes(token); }
function generateWalletId() { return 'RC-' + String(Math.floor(Math.random() * 900000 + 100000)).padStart(6, '0'); }
function generateEthAddress() { return '0x' + Array(40).fill(0).map(() => Math.floor(Math.random() * 16).toString(16)).join(''); }
function calculateExpiry(tier) {
    const weeks = { 1: 6, 2: 8, 3: 9, 4: 12, 5: 15, 6: 18, 7: 21 }[parseInt(tier)] || 6;
    const d = new Date();
    d.setDate(d.getDate() + weeks * 7);
    return d.toISOString();
}
async function getBalance(wallet, token) {
    const row = await dbGet('SELECT amount FROM balances WHERE wallet = ? AND token = ?', [wallet, token]);
    return row ? row.amount : 0;
}
async function updateBalance(wallet, amount, token) {
    if (!wallet) return 0;
    const current = await getBalance(wallet, token);
    const newBalance = current + amount;
    await dbRun('INSERT OR REPLACE INTO balances (wallet, token, amount) VALUES (?, ?, ?)', [wallet, token, newBalance]);
    return newBalance;
}
async function walletExists(wallet) {
    const row = await dbGet('SELECT wallet FROM members WHERE wallet = ?', [wallet]);
    return !!row;
}
async function isAdmin(wallet) {
    if (ADMIN_WALLETS.includes(wallet)) return true;
    const row = await dbGet('SELECT role FROM members WHERE wallet = ? AND role = ?', [wallet, 'admin']);
    return !!row;
}

// ============================================
// SEED ADMIN MEMBER ROW
// ============================================
async function seedAdminMember() {
    const adminWallet = WALLET_IDS.ADMIN;
    const adminName = process.env.ADMIN_NAME || 'RC Admin';
    const adminUsername = process.env.ADMIN_USERNAME || 'admin';

    try {
        const existing = await dbGet('SELECT wallet FROM members WHERE wallet = ?', [adminWallet]);
        if (existing) {
            console.log(`✅ Admin member row exists: ${adminWallet}`);
            return;
        }

        await dbRun(
            `INSERT INTO members (wallet, eth, name, username, email, phone, address, role, tier, amount_paid, token_balance, registered_at, expiry_date, status, client_secret, crypto_wallet, crypto_network)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [adminWallet, '', adminName, adminUsername, '', '', '', 'admin', 0, 0, 0, Date.now(), '', 'active',
             null, process.env.ADMIN_CRYPTO_WALLET || null, process.env.ADMIN_CRYPTO_NETWORK || 'trc20']
        );

        console.log(`✅ Admin member row seeded: ${adminWallet} (${adminName})`);

        await addToLedger({
            type: 'ADMIN_MEMBER_SEEDED',
            from: 'SYSTEM',
            to: adminWallet,
            amount: 0,
            token: 'RGT',
            extra: { wallet: adminWallet, name: adminName, username: adminUsername, role: 'admin', event: 'SYSTEM_BOOTSTRAP' },
            debit: false, credit: false
        });
    } catch (err) {
        console.error('⚠️  Admin member seed failed:', err.message);
    }
}

// ============================================
// EXCHANGE RATE HELPERS
// ============================================
async function getExchangeRate(token) {
    const row = await dbGet('SELECT rate_ngn FROM exchange_rates WHERE token = ?', [token]);
    return row ? row.rate_ngn : null;
}
async function getAllExchangeRates() {
    return await dbAll('SELECT * FROM exchange_rates ORDER BY token');
}
async function updateExchangeRate(token, rate_ngn, currency, adminWallet) {
    await dbRun(
        `INSERT OR REPLACE INTO exchange_rates (token, rate_ngn, currency, updated_by, updated_at) VALUES (?, ?, ?, ?, ?)`,
        [token, rate_ngn, currency || 'NGN', adminWallet, Date.now()]
    );
    return { ok: true, token, rate_ngn, currency: currency || 'NGN' };
}

// ============================================
// QUIDAX WRAPPER
// ============================================
async function quidaxBuyAndSend({ ngnAmount, destination, network, asset }) {
    const apiKey = process.env.QUIDAX_API_KEY;
    if (!apiKey) {
        const rate = parseFloat(process.env.USDT_NGN_RATE) || 1550;
        const usdtReceived = ngnAmount / rate;
        const simTxHash = 'SIM_' + Date.now() + '_' + Math.random().toString(36).substr(2, 8);
        console.log(`⚠️  [SIMULATION] Buy ${usdtReceived.toFixed(2)} ${asset} with ₦${ngnAmount} → ${destination}`);
        return {
            ok: true, simulated: true,
            tx_hash: simTxHash,
            reference: 'SIM_REF_' + Date.now(),
            order_id: 'SIM_ORDER_' + Date.now(),
            received: usdtReceived
        };
    }
    try {
        const buyResponse = await fetch('https://www.quidax.com/api/v1/users/me/buy', {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ currency: 'ngn', amount: ngnAmount, target_currency: asset.toLowerCase() })
        });
        if (!buyResponse.ok) return { ok: false, error: 'Buy failed: ' + (await buyResponse.text()).substring(0, 200) };
        const buyResult = await buyResponse.json();
        if (buyResult.status !== 'success') return { ok: false, error: buyResult.message || 'Buy rejected' };
        const received = buyResult.data.received;
        const sendResponse = await fetch('https://www.quidax.com/api/v1/users/me/withdraws', {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ currency: asset.toLowerCase(), amount: received, network: network, address: destination })
        });
        if (!sendResponse.ok) return { ok: false, error: 'Send failed: ' + (await sendResponse.text()).substring(0, 200) };
        const sendResult = await sendResponse.json();
        if (sendResult.status !== 'success') return { ok: false, error: sendResult.message || 'Send rejected' };
        return {
            ok: true, simulated: false,
            tx_hash: sendResult.data.tx_hash || sendResult.data.hash,
            reference: sendResult.data.reference,
            order_id: sendResult.data.id,
            received: received
        };
    } catch (err) {
        return { ok: false, error: 'Quidax API error: ' + err.message };
    }
}

// ============================================
// SETTLEMENT SLIP PROCESSING
// ============================================
async function generateSettlementSlip(wallet, token, amount, bankDetails) {
    const rate = await getExchangeRate(token);
    if (!rate) return { ok: false, error: 'No exchange rate set for ' + token };
    const ngnValue = amount * rate;
    const slipId = 'SLIP-' + Date.now() + '-' + Math.random().toString(36).substr(2, 6);

    let cryptoWallet, cryptoNetwork;
    if (ADMIN_WALLETS.includes(wallet)) {
        cryptoWallet = ADMIN_CRYPTO_WALLET;
        cryptoNetwork = ADMIN_CRYPTO_NETWORK;
        if (!cryptoWallet) return { ok: false, error: 'Admin wallet not configured (ADMIN_CRYPTO_WALLET missing in .env)' };
    } else {
        const member = await dbGet('SELECT crypto_wallet, crypto_network FROM members WHERE wallet = ?', [wallet]);
        if (!member) return { ok: false, error: 'Member not found' };
        if (!member.crypto_wallet) return { ok: false, error: 'No crypto wallet set for member' };
        cryptoWallet = member.crypto_wallet;
        cryptoNetwork = member.crypto_network;
    }

    await dbRun(
        `INSERT INTO settlement_slips (slip_id, wallet, token, token_amount, rate, ngn_value, bank_details, crypto_wallet, crypto_network, status, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)`,
        [slipId, wallet, token, amount, rate, ngnValue,
         JSON.stringify(bankDetails || {}), cryptoWallet, cryptoNetwork, Date.now()]
    );
    return {
        ok: true, slip_id: slipId, token, token_amount: amount,
        rate, ngn_value: ngnValue, crypto_wallet: cryptoWallet, network: cryptoNetwork
    };
}

async function processSettlementSlip(slipId) {
    const slip = await dbGet('SELECT * FROM settlement_slips WHERE slip_id = ?', [slipId]);
    if (!slip) return { ok: false, error: 'Slip not found' };
    if (slip.status !== 'pending') return { ok: false, error: 'Slip already processed: ' + slip.status };

    const userBalance = await getBalance(slip.wallet, slip.token);
    if (userBalance < slip.token_amount) {
        await dbRun('UPDATE settlement_slips SET status = ?, error = ? WHERE slip_id = ?', ['failed', 'Insufficient balance', slipId]);
        return { ok: false, error: 'Insufficient balance' };
    }

    const quidaxResult = await quidaxBuyAndSend({
        ngnAmount: slip.ngn_value,
        destination: slip.crypto_wallet,
        network: slip.crypto_network,
        asset: 'USDT'
    });

    if (!quidaxResult.ok) {
        await dbRun('UPDATE settlement_slips SET status = ?, error = ? WHERE slip_id = ?', ['failed', quidaxResult.error, slipId]);
        return { ok: false, error: quidaxResult.error };
    }

    await dbRun(
        `UPDATE settlement_slips SET status = 'completed', quidax_tx_hash = ?, quidax_reference = ?, quidax_order_id = ?, usdt_amount = ?, completed_at = ? WHERE slip_id = ?`,
        [quidaxResult.tx_hash, quidaxResult.reference, quidaxResult.order_id, quidaxResult.received, Date.now(), slipId]
    );

    await updateBalance(slip.wallet, -slip.token_amount, slip.token);

    await addToLedger({
        type: 'CASHOUT_COMPLETED', from: slip.wallet, to: slip.crypto_wallet,
        amount: slip.token_amount, token: slip.token,
        extra: {
            slip_id: slipId, rate: slip.rate, ngn_value: slip.ngn_value,
            usdt_amount: quidaxResult.received, crypto_network: slip.crypto_network,
            quidax_tx_hash: quidaxResult.tx_hash, simulated: quidaxResult.simulated || false,
            event: 'COIN_TO_USDT_CONVERSION'
        },
        debit: false, credit: false
    });

    if (io) {
        io.emit('packet', {
            type: 'CASHOUT_COMPLETED', to_wallet: slip.wallet, slip_id: slipId,
            token: slip.token, token_amount: slip.token_amount,
            ngn_value: slip.ngn_value, usdt_amount: quidaxResult.received,
            tx_hash: quidaxResult.tx_hash, simulated: quidaxResult.simulated || false,
            message: quidaxResult.simulated
                ? `✅ SIMULATION: ${slip.token_amount} ${slip.token} → ${quidaxResult.received.toFixed(2)} USDT`
                : `✅ CASHOUT: ${slip.token_amount} ${slip.token} → ${quidaxResult.received.toFixed(2)} USDT sent`,
            timestamp: Date.now()
        });
    }
    return { ok: true, slip_id: slipId, tx_hash: quidaxResult.tx_hash, usdt_sent: quidaxResult.received, simulated: quidaxResult.simulated || false };
}

// ============================================
// LEDGER — v6.5
// ============================================
async function addToLedger(entry) {
    const txId = 'TX_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);
    const timestamp = Date.now();
    const instruction = entry.instruction || (entry.extra && entry.extra.instruction) || null;
    const instructionJson = instruction ? JSON.stringify(instruction) : null;
    const packetSignature = entry.sig || entry.packet_signature || null;
    const packetId = entry.packet_id || null;

    await dbRun(
        `INSERT INTO ledger (tx_id, type, from_wallet, to_wallet, amount, token, timestamp, status, extra, sync_status, instruction, packet_signature, packet_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [txId, entry.type,
         entry.from || entry.from_wallet || null,
         entry.to || entry.to_wallet || null,
         entry.amount || 0,
         entry.token || 'RGT',
         timestamp,
         entry.status || 'confirmed',
         typeof entry.extra === 'string' ? entry.extra : JSON.stringify(entry.extra || {}),
         IS_CLOUD ? 'synced' : 'pending_sync',
         instructionJson, packetSignature, packetId]
    );

    if (entry.from && entry.amount && entry.debit !== false) await updateBalance(entry.from, -entry.amount, entry.token || 'RGT');
    if (entry.to && entry.amount && entry.credit !== false) await updateBalance(entry.to, entry.amount, entry.token || 'RGT');

    if (io) {
        const broadcast = {
            tx_id: txId, type: entry.type,
            from: entry.from || entry.from_wallet, to: entry.to || entry.to_wallet,
            from_wallet: entry.from || entry.from_wallet, to_wallet: entry.to || entry.to_wallet,
            amount: entry.amount, token: entry.token,
            timestamp: timestamp, extra: entry.extra, instruction: instruction
        };
        io.emit('ledger_entry', broadcast);
        io.emit('packet', broadcast);
    }

    if (!IS_CLOUD) queueForFallbackSync(txId, entry);
    return txId;
}

async function getLedger(limit = 100) {
    return await dbAll('SELECT * FROM ledger ORDER BY timestamp DESC LIMIT ?', [limit]);
}

async function queueForFallbackSync(txId, entry) {
    if (IS_CLOUD) return;
    try {
        const response = await fetch(`${FALLBACK_SERVER_URL}/api/sync/ledger`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                tx_id: txId, type: entry.type,
                from_wallet: entry.from || entry.from_wallet || null,
                to_wallet: entry.to || entry.to_wallet || null,
                amount: entry.amount || 0, token: entry.token || 'RGT',
                timestamp: Date.now(), extra: entry.extra || {},
                instruction: entry.instruction || null
            }),
            signal: AbortSignal.timeout(8000)
        });
        if (response.ok) await dbRun(`UPDATE ledger SET sync_status = 'synced' WHERE tx_id = ?`, [txId]);
        else throw new Error('Fallback rejected');
    } catch (err) {
        await dbRun(
            `INSERT INTO sync_queue (tx_id, data, attempts, created_at, status) VALUES (?, ?, ?, ?, ?)`,
            [txId, JSON.stringify(entry), 0, Date.now(), 'pending']
        );
    }
}

// ============================================
// LEDGER APPLY (for rebuild)
// ============================================
async function applyLedgerEntry(entry) {
    const txId = entry.tx_id;
    if (txId) {
        const applied = await dbGet('SELECT tx_id FROM applied_tx WHERE tx_id = ?', [txId]);
        if (applied) return { skipped: true, tx_id: txId };
    }
    const extra = typeof entry.extra === 'string'
        ? (() => { try { return JSON.parse(entry.extra); } catch(e) { return {}; } })()
        : (entry.extra || {});

    switch (entry.type) {
        case 'USER_REGISTERED':
            if (extra.wallet) {
                await dbRun(
                    `INSERT OR REPLACE INTO members (wallet, eth, name, username, email, phone, address, role, tier, amount_paid, token_balance, registered_at, expiry_date, status, client_secret, crypto_wallet, crypto_network)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [extra.wallet, extra.eth || '', extra.name || '', extra.username || '',
                     extra.email || '', extra.phone || '', extra.address || '',
                     extra.role || 'user', extra.tier || 1, extra.amount_paid || 0,
                     extra.token_amount || 0, extra.registered_at || entry.timestamp,
                     extra.expiry_date || '', extra.status || 'active',
                     extra.client_secret || null, extra.crypto_wallet || null, extra.crypto_network || null]
                );
            }
            break;
        case 'TRANSFER': case 'P2P_TRANSFER': case 'PEER_SETTLEMENT':
        case 'MASS_PAY': case 'MASS_PAY_RECEIVED':
            if (entry.from_wallet && entry.amount) await updateBalance(entry.from_wallet, -entry.amount, entry.token || 'RGT');
            if (entry.to_wallet && entry.amount) await updateBalance(entry.to_wallet, entry.amount, entry.token || 'RGT');
            if (extra.bonus_receiver) await updateBalance(entry.to_wallet, extra.bonus_receiver, entry.token || 'RGT');
            if (extra.bonus_sender) await updateBalance(entry.from_wallet, extra.bonus_sender, entry.token || 'RGT');
            if (extra.bonus_crown) await updateBalance(WALLET_IDS.CROWN_BANK, extra.bonus_crown, entry.token || 'RGT');
            break;
        case 'CASHOUT_COMPLETED':
        case 'MASS_PAYOUT_EXTERNAL_ITEM':
            if (entry.from_wallet && entry.amount) await updateBalance(entry.from_wallet, -entry.amount, entry.token || 'RGT');
            break;
        case 'NFT_MINT': case 'SHARE_CERTIFICATE_ISSUED':
            if (extra.id || extra.nft_id) {
                await dbRun(
                    `INSERT OR REPLACE INTO nfts (id, artist_name, artist_wallet, total_shares, shares_available, price_per_share, token, slot, monthly_return, share_per_unit, image_url, description, benefits, status, minted_by, minted_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [extra.id || extra.nft_id, extra.artist_name || '', extra.artist_wallet || '',
                     extra.total_shares || 0, extra.total_shares || 0, extra.price_per_share || 0,
                     extra.token || 'RGT', extra.slot || '', extra.monthly_return || 0,
                     extra.share_per_unit || null, extra.image_url || '', extra.description || '',
                     extra.benefits || '', 'active', entry.from_wallet, entry.timestamp]
                );
            }
            break;
        case 'FEED_POST':
            if (extra.id) {
                await dbRun(
                    `INSERT OR REPLACE INTO feed_posts (id, from_wallet, message, image, category, timestamp, attendCount, wantCount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
                    [extra.id, entry.from_wallet || 'UNKNOWN', extra.message || '',
                     extra.image || null, extra.category || 'Client',
                     extra.timestamp || entry.timestamp, extra.attendCount || 0, extra.wantCount || 0]
                );
            }
            break;
        case 'P2P_MSG':
            if (entry.from_wallet && entry.to_wallet) {
                await dbRun(
                    `INSERT INTO messages (from_wallet, to_wallet, body, image, timestamp, read) VALUES (?, ?, ?, ?, ?, ?)`,
                    [entry.from_wallet, entry.to_wallet, extra.message || '', extra.image || null, entry.timestamp, 0]
                );
            }
            break;
        case 'BROADCAST_MSG':
            if (entry.from_wallet) {
                await dbRun(
                    `INSERT INTO broadcasts (from_wallet, body, image, timestamp) VALUES (?, ?, ?, ?)`,
                    [entry.from_wallet, extra.message || '', extra.image || null, entry.timestamp]
                );
            }
            break;
        case 'CRYPTO_WALLET_SET': case 'CRYPTO_WALLET_UPDATED':
            if (entry.to_wallet && extra.crypto_wallet) {
                await dbRun(`UPDATE members SET crypto_wallet = ?, crypto_network = ? WHERE wallet = ?`,
                    [extra.crypto_wallet, extra.crypto_network || '', entry.to_wallet]);
            }
            break;
        case 'FREEZE':    if (entry.to_wallet) await dbRun(`UPDATE members SET status = 'frozen' WHERE wallet = ?`, [entry.to_wallet]); break;
        case 'UNFREEZE':  if (entry.to_wallet) await dbRun(`UPDATE members SET status = 'active' WHERE wallet = ?`, [entry.to_wallet]); break;
        case 'BLACKLIST': if (entry.to_wallet) await dbRun(`UPDATE members SET status = 'blacklisted' WHERE wallet = ?`, [entry.to_wallet]); break;
        case 'UNBLACKLIST': if (entry.to_wallet) await dbRun(`UPDATE members SET status = 'active' WHERE wallet = ?`, [entry.to_wallet]); break;
        case 'NEURAL_NOTE_CREATED': case 'NEURAL_NOTE_UPDATED':
            if (extra.noteId) {
                await dbRun(
                    `INSERT OR REPLACE INTO notes (id, wallet, title, content, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
                    [extra.noteId, entry.from_wallet, extra.title || '', extra.content || '', entry.timestamp, Date.now()]
                );
            }
            break;
        case 'NEURAL_NOTE_DELETED': if (extra.noteId) await dbRun(`DELETE FROM notes WHERE id = ?`, [extra.noteId]); break;
        case 'NEURAL_NOTES_CLEARED': await dbRun(`DELETE FROM notes WHERE wallet = ?`, [entry.from_wallet]); break;
        case 'WORK_REGISTER':
            if (extra.work_id) {
                await dbRun(
                    `INSERT OR REPLACE INTO creative_works (id, catalog_id, work_type, title, creator_wallet, creator_name, co_creators, genre, status, registered_by, registered_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [extra.work_id, extra.catalog_id, extra.work_type, extra.title,
                     extra.creator_wallet, extra.creator_name || '',
                     JSON.stringify(extra.co_creators || []), extra.genre || '',
                     'registered', entry.from_wallet, entry.timestamp]
                );
            }
            break;
        case 'MERCH_REGISTER':
            if (extra.merch_id) {
                await dbRun(
                    `INSERT OR REPLACE INTO merch_registry (id, catalog_id, merch_type, title, creator_wallet, creator_name, linked_work_id, price, token, stock, status, registered_by, registered_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [extra.merch_id, extra.catalog_id, extra.merch_type, extra.title,
                     extra.creator_wallet, extra.creator_name || '',
                     extra.linked_work_id || null, extra.price || 0,
                     extra.token || 'RGT', extra.stock || 0,
                     'registered', entry.from_wallet, entry.timestamp]
                );
            }
            break;
        case 'SHOW_TICKET_PURCHASED':
            if (extra.show_id && entry.to_wallet) {
                // Handled in handler already
            }
            break;
    }
    if (txId) {
        try { await dbRun('INSERT OR IGNORE INTO applied_tx (tx_id, applied_at) VALUES (?, ?)', [txId, Date.now()]); } catch (e) {}
    }
    return { applied: true, tx_id: txId };
}

// ============================================
// ISSUANCE / REDEMPTION / BRIDGE
// ============================================
async function issueTokens(token, amount, beneficiary, reason, refTxId = null, metadata = {}) {
    if (!token || !amount || amount <= 0 || !beneficiary) return { ok: false, error: 'Invalid params' };
    if (!isValidToken(token)) return { ok: false, error: 'Invalid token' };
    const newBalance = await updateBalance(beneficiary, +amount, token);
    await dbRun(
        `INSERT INTO issuance_log (token, amount, reason, beneficiary, ref_tx_id, metadata, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [token, amount, reason, beneficiary, refTxId, JSON.stringify(metadata || {}), Date.now()]
    );
    return { ok: true, token, amount, beneficiary, newBalance };
}
async function redeemTokens(token, amount, fromWallet, reason, fiatAmount = 0, currency = 'NGN', refTxId = null) {
    if (!token || !amount || amount <= 0 || !fromWallet) return { ok: false, error: 'Invalid params' };
    const current = await getBalance(fromWallet, token);
    if (current < amount) return { ok: false, error: 'Insufficient balance' };
    const newBalance = await updateBalance(fromWallet, -amount, token);
    await dbRun(
        `INSERT INTO redemption_log (token, amount, reason, from_wallet, fiat_amount, currency, ref_tx_id, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [token, amount, reason, fromWallet, fiatAmount, currency, refTxId, Date.now()]
    );
    return { ok: true, token, amount, fromWallet, newBalance };
}
async function bridgeTokens(token, amount, fromWallet, toChain, toAddress, direction = 'out', refTxId = null) {
    let newBalance = await getBalance(fromWallet, token);
    if (direction === 'out') {
        if (newBalance < amount) return { ok: false, error: 'Insufficient balance' };
        newBalance = await updateBalance(fromWallet, -amount, token);
    } else {
        newBalance = await updateBalance(fromWallet, +amount, token);
    }
    await dbRun(
        `INSERT INTO bridge_log (token, amount, direction, from_wallet, to_wallet, to_chain, to_address, tx_hash, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [token, amount, direction, fromWallet, null, toChain || null, toAddress || null, refTxId, Date.now()]
    );
    return { ok: true, token, amount, fromWallet, direction, newBalance };
}
async function getSupply(token) {
    const issuedRow = await dbGet(`SELECT COALESCE(SUM(amount), 0) as total FROM issuance_log WHERE token = ?`, [token]);
    const redeemedRow = await dbGet(`SELECT COALESCE(SUM(amount), 0) as total FROM redemption_log WHERE token = ?`, [token]);
    return { token, issued: issuedRow ? issuedRow.total : 0, redeemed: redeemedRow ? redeemedRow.total : 0, circulating: (issuedRow ? issuedRow.total : 0) - (redeemedRow ? redeemedRow.total : 0) };
}
async function getAllSupplies() {
    const supplies = {};
    for (const token of VALID_TOKENS) supplies[token] = await getSupply(token);
    return supplies;
}

// ============================================
// PACKET HANDLERS — REGISTRATION
// ============================================
async function handleRegistrationRequest(packet) {
    const { name, username, email, phone, address, role, tier, voucher, extra_services } = packet;
    if (!name || !username || !voucher) return { success: false, error: 'Name, username, voucher required' };
    await dbRun(
        `INSERT INTO pending_registrations (name, username, email, phone, address, role, tier, voucher, extra_services, submitted_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [name, username, email || '', phone || '', address || '', role || 'user', tier || null, voucher,
         JSON.stringify(extra_services || []), Date.now(), 'pending']);
    await addToLedger({
        type: 'REGISTRATION_REQUEST', from: 'REGISTRATION_FORM', to: 'ONBOARDING_PENDING',
        amount: 0, token: 'RGT', extra: { name, username, role, tier, voucher },
        debit: false, credit: false,
        instruction: packet.instruction || null,
        packet_id: packet.packet_id || null
    });
    return { success: true, message: 'Registration submitted — awaiting admin approval' };
}

async function handleUserRegistration(packet) {
    const data = packet.data || packet;
    const { name, username, email, phone, address, role, tier, amount_paid, adminWallet } = data;
    if (!await isAdmin(packet.from_wallet)) return { success: false, error: 'Unauthorized' };
    const wallet = packet.wallet || data.wallet || generateWalletId();
    const eth = packet.eth || data.eth || generateEthAddress();
    const tokenAmount = packet.token_amount || data.token_amount || (amount_paid / 520);
    const expiryDate = packet.expiry_date || data.expiry_date || calculateExpiry(tier);
    const clientSecret = packet.client_secret || data.client_secret || null;

    if (!name || !username || !amount_paid) return { success: false, error: 'Missing fields' };
    if (await dbGet('SELECT wallet FROM members WHERE wallet = ?', [wallet])) return { success: false, error: 'Wallet exists' };
    if (await dbGet('SELECT username FROM members WHERE username = ?', [username])) return { success: false, error: 'Username exists' };

    await dbRun(
        `INSERT INTO members (wallet, eth, name, username, email, phone, address, role, tier, amount_paid, token_balance, registered_at, expiry_date, status, client_secret) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [wallet, eth, name, username, email || '', phone || '', address || '', role || 'user', tier, amount_paid, 0, Date.now(), expiryDate, 'active', clientSecret]
    );
    await addToLedger({
        type: 'USER_REGISTERED', from: 'SYSTEM', to: wallet,
        amount: amount_paid, token: 'NGN',
        extra: { wallet, eth, name, username, email: email || '', phone: phone || '', address: address || '', role: role || 'user', tier, amount_paid, token_amount: tokenAmount, exchange_rate: 520, expiry_date: expiryDate, registered_at: Date.now(), status: 'active', admin: adminWallet || 'SYSTEM', client_secret: clientSecret },
        debit: false, credit: false,
        instruction: packet.instruction || null,
        packet_id: packet.packet_id || null
    });
    return { success: true, walletId: wallet, ethAddress: eth, tokenAmount, clientSecret };
}

async function handleWalletDeleted(packet) {
    const { wallet, name } = packet;
    if (!await isAdmin(packet.from_wallet)) return { success: false, error: 'Unauthorized' };
    if (!wallet) return { success: false, error: 'Wallet required' };
    await dbRun('DELETE FROM members WHERE wallet = ?', [wallet]);
    await dbRun('DELETE FROM balances WHERE wallet = ?', [wallet]);
    await addToLedger({ type: 'WALLET_DELETED', from: 'ADMIN', to: wallet, amount: 0, token: 'RGT', extra: { name: name || '', wallet }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}

async function handleRatesUpdated(packet) {
    if (!await isAdmin(packet.from_wallet)) return { success: false, error: 'Unauthorized' };
    await addToLedger({
        type: 'RATES_UPDATED', from: packet.from_wallet || 'ADMIN', to: 'SYSTEM',
        amount: 0, token: 'RGT',
        extra: { exchange: packet.exchange_rate, extract: packet.extract_percent, farming: packet.farming_percent, penalty: packet.universal_penalty },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true };
}

// ============================================
// PACKET HANDLERS — TRANSACTIONS
// ============================================
async function handleTransfer(packet) {
    const { from_wallet, to_wallet, amount, token } = packet;
    const guard = await guardActive(from_wallet);
    if (!guard.ok) return { success: false, error: guard.error };
    if (!await walletExists(to_wallet)) return { success: false, error: 'Recipient not found' };
    if (from_wallet === to_wallet) return { success: false, error: 'Self-transfer not allowed' };
    if (amount <= 0) return { success: false, error: 'Invalid amount' };
    if (!isValidToken(token)) return { success: false, error: 'Invalid token' };
    const balance = await getBalance(from_wallet, token);
    if (balance < amount) return { success: false, error: 'Insufficient balance' };

    await updateBalance(from_wallet, -amount, token);
    await updateBalance(to_wallet, amount, token);

    const bonusRecipient = amount * 0.30;
    const bonusSender = amount * 0.20;
    const bonusCrown = amount * 0.10;

    if (bonusRecipient > 0) await updateBalance(to_wallet, bonusRecipient, token);
    if (bonusSender > 0) await updateBalance(from_wallet, bonusSender, token);
    if (bonusCrown > 0) await updateBalance(WALLET_IDS.CROWN_BANK, bonusCrown, token);

    await addToLedger({
        type: 'TRANSFER', from: from_wallet, to: to_wallet, amount, token,
        extra: { bonus_receiver: bonusRecipient, bonus_sender: bonusSender, bonus_crown: bonusCrown },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true };
}

async function handleMassPay(packet) {
    const { from_wallet, recipients, token } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    if (!recipients || recipients.length === 0) return { success: false, error: 'No recipients' };
    const totalAmount = recipients.reduce((sum, r) => sum + (r.amount || 0), 0);
    const vaultBalance = await getBalance(WALLET_IDS.ADMIN, token);
    if (vaultBalance < totalAmount) return { success: false, error: 'Insufficient VAULT balance' };
    await updateBalance(WALLET_IDS.ADMIN, -totalAmount, token);

    const delivered = [];
    for (const recipient of recipients) {
        if (!await walletExists(recipient.wallet)) continue;
        const guard = await guardActive(recipient.wallet);
        if (!guard.ok) continue;
        await updateBalance(recipient.wallet, recipient.amount, token);
        await dbRun(`UPDATE pending_funding SET status = 'funded' WHERE wallet = ?`, [recipient.wallet]);
        const txId = await addToLedger({
            type: 'MASS_PAY', from: WALLET_IDS.ADMIN, to: recipient.wallet, amount: recipient.amount, token,
            extra: { name: recipient.name || '' }, debit: false, credit: false,
            instruction: packet.instruction || null, packet_id: packet.packet_id || null
        });
        if (io) io.emit('packet', { type: 'MASS_PAY_RECEIVED', to_wallet: recipient.wallet, from_wallet: WALLET_IDS.ADMIN, amount: recipient.amount, token, tx_id: txId, timestamp: Date.now() });
        delivered.push({ wallet: recipient.wallet, amount: recipient.amount });
    }
    await addToLedger({
        type: 'MASS_PAY_BATCH', from: from_wallet, to: 'MULTIPLE',
        amount: totalAmount, token, extra: { recipients: recipients.length, delivered },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true, total: totalAmount, count: delivered.length };
}

async function handleMassPayBatch(packet) {
    const { from_wallet, total_amount, recipient_count, token } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    await addToLedger({
        type: 'MASS_PAY_BATCH', from: from_wallet, to: 'MULTIPLE',
        amount: total_amount || 0, token: token || 'RGT',
        extra: { recipients: recipient_count || 0 }, debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true };
}

async function handleSwap(packet) {
    const { from_wallet, to_wallet, from_token, to_token } = packet;
    const amountA = parseFloat(packet.amount_a || packet.amount || 0);
    const amountB = parseFloat(packet.amount_b || packet.amount || 0);
    
    const guard = await guardActive(from_wallet);
    if (!guard.ok) return { success: false, error: guard.error };
    if (!isValidToken(from_token)) return { success: false, error: 'Invalid from_token' };
    if (!isValidToken(to_token)) return { success: false, error: 'Invalid to_token' };
    if (from_token === to_token) return { success: false, error: 'Cannot swap same token' };
    if (amountA <= 0) return { success: false, error: 'Invalid amount A' };
    if (amountB <= 0) return { success: false, error: 'Invalid amount B' };

    const balanceA = await getBalance(from_wallet, from_token);
    if (balanceA < amountA) return { success: false, error: 'Insufficient ' + from_token };

    const vaultB = await getBalance(WALLET_IDS.ADMIN, to_token);
    if (vaultB < amountB) return { success: false, error: 'Vault has insufficient ' + to_token };

    await updateBalance(from_wallet, -amountA, from_token);
    await updateBalance(WALLET_IDS.ADMIN, +amountA, from_token);

    await updateBalance(WALLET_IDS.ADMIN, -amountB, to_token);
    await updateBalance(to_wallet || from_wallet, +amountB, to_token);

    await addToLedger({
        type: 'SWAP',
        from: from_wallet,
        to: to_wallet || from_wallet,
        amount: amountA,
        token: from_token,
        extra: { to_token, amount_a: amountA, amount_b: amountB, from_token, vault_in: from_token, vault_out: to_token },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true, paid: amountA, paid_token: from_token, received: amountB, received_token: to_token };
}

async function handlePeerTransfer(packet) {
    const { from_wallet, to_wallet, amount, token } = packet;
    const guard = await guardActive(from_wallet);
    if (!guard.ok) return { success: false, error: guard.error };
    if (!await walletExists(to_wallet)) return { success: false, error: 'Recipient not found' };
    if (amount <= 0) return { success: false, error: 'Invalid amount' };
    const balance = await getBalance(from_wallet, token || 'RGT');
    if (balance < amount) return { success: false, error: 'Insufficient balance' };
    await updateBalance(from_wallet, -amount, token || 'RGT');
    await updateBalance(to_wallet, amount, token || 'RGT');
    await addToLedger({ type: 'P2P_TRANSFER', from: from_wallet, to: to_wallet, amount, token: token || 'RGT', debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}

async function deductWithFallback(wallet, amount, preferredToken) {
    const tokenOrder = [preferredToken, 'RGT', 'RCT', 'IRT', 'RT', 'ET'].filter((v, i, a) => a.indexOf(v) === i);
    for (const token of tokenOrder) {
        const balance = await getBalance(wallet, token);
        if (balance >= amount) {
            await updateBalance(wallet, -amount, token);
            return { success: true, token, deducted: amount };
        }
    }
    return { success: false, error: 'Insufficient in all tokens' };
}

async function handlePurchase(packet) {
    const { from_wallet, to_wallet, product_id, product_type } = packet;
    const guard = await guardActive(from_wallet);
    if (!guard.ok) return { success: false, error: guard.error, silent: true };
    let amount = packet.amount, reward_amount = packet.reward_amount || 0, token = packet.token;
    let sellerWallet = to_wallet;

    // ----- Virtual Show ticket purchase (v6.5: generates ticket_id) -----
    if (product_type === 'virtual_show' && product_id) {
        const show = await dbGet('SELECT * FROM shows WHERE id = ?', [product_id]);
        if (!show) return { success: false, error: 'Show not found' };

        const ticketPrice = show.ticket_price;
        const rewardAmt = show.attendee_reward;
        const ticketToken = show.ticket_token || 'RGT';
        const rewardTok = show.reward_token || 'RGT';

        const existing = await dbGet('SELECT * FROM show_attendance WHERE show_id = ? AND attendee_wallet = ?', [product_id, from_wallet]);
        if (existing && existing.status === 'attending') {
            return { success: false, error: 'Already attending this show', ticket_id: existing.ticket_id };
        }

        if (ticketPrice > 0) {
            const userBal = await getBalance(from_wallet, ticketToken);
            if (userBal < ticketPrice) return { success: false, error: 'Insufficient ' + ticketToken + ' for ticket' };
        }

        if (rewardAmt > 0) {
            const vaultBal = await getBalance(WALLET_IDS.ADMIN, rewardTok);
            if (vaultBal < rewardAmt) return { success: false, error: 'Reward pool empty for ' + rewardTok };
        }

        if (ticketPrice > 0) await updateBalance(from_wallet, -ticketPrice, ticketToken);
        if (ticketPrice > 0 && show.artist_wallet) {
            await updateBalance(show.artist_wallet, +ticketPrice, ticketToken);
        }
        if (rewardAmt > 0) {
            await updateBalance(WALLET_IDS.ADMIN, -rewardAmt, rewardTok);
            await updateBalance(from_wallet, +rewardAmt, rewardTok);
        }

        // Generate the ticket
        const ticketId = (existing && existing.ticket_id) || generateTicketId(product_id, from_wallet);

        await dbRun(
            `INSERT OR REPLACE INTO show_attendance 
             (show_id, attendee_wallet, ticket_paid, ticket_token, reward_received, reward_token, purchased_at, status, ticket_id, admission_status)
             VALUES (?, ?, ?, ?, ?, ?, ?, 'attending', ?, 'pending')`,
            [product_id, from_wallet, ticketPrice, ticketToken, rewardAmt, rewardTok, Date.now(), ticketId]
        );

        await dbRun(
            `UPDATE shows SET attendee_count = attendee_count + 1, 
             total_ticket_revenue = total_ticket_revenue + ?, 
             total_reward_paid = total_reward_paid + ? 
             WHERE id = ?`,
            [ticketPrice, rewardAmt, product_id]
        );

        await addToLedger({
            type: 'SHOW_TICKET_PURCHASED',
            from: from_wallet, to: show.artist_wallet,
            amount: ticketPrice, token: ticketToken,
            extra: {
                show_id: product_id, show_title: show.title,
                reward_amount: rewardAmt, reward_token: rewardTok,
                ticket_id: ticketId,
                venue: show.venue || null,
                venue_address: show.venue_address || null,
                scheduled_at: show.scheduled_at
            },
            debit: false, credit: false,
            packet_id: packet.packet_id || null
        });

        if (rewardAmt > 0) {
            await addToLedger({
                type: 'SHOW_ATTENDEE_REWARD',
                from: WALLET_IDS.ADMIN, to: from_wallet,
                amount: rewardAmt, token: rewardTok,
                extra: { show_id: product_id, show_title: show.title, ticket_id: ticketId },
                debit: false, credit: false,
                packet_id: packet.packet_id ? packet.packet_id + '_reward' : null
            });
        }

        return {
            success: true,
            show_id: product_id,
            ticket_id: ticketId,
            ticket_paid: ticketPrice,
            ticket_token: ticketToken,
            reward_received: rewardAmt,
            reward_token: rewardTok
        };
    }

    // ----- Regular product purchase -----
    if (!amount || amount === 0 || !token || token === 'AUTO') {
        const lastSync = await dbGet(`SELECT * FROM ledger WHERE type = 'REGISTRY_SYNC' ORDER BY timestamp DESC LIMIT 1`);
        if (!lastSync) {
            await addToLedger({
                type: 'PURCHASE',
                from: from_wallet,
                to: sellerWallet || 'SYSTEM',
                amount: 0,
                token: 'RGT',
                extra: { product_id, product_type, reward_amount, note: 'No registry — treated as free interest' },
                debit: false, credit: false,
                instruction: packet.instruction || null, packet_id: packet.packet_id || null
            });
            return { success: true, amount: 0, reward_amount: 0, token: 'RGT', note: 'Free interest registered' };
        }
        const extra = typeof lastSync.extra === 'string' ? JSON.parse(lastSync.extra) : lastSync.extra;
        const registry = extra.registry || [];
        const registryRow = registry.find(r => r.location === packet.registry_location && r.buttonType === packet.registry_button && r.targetWallet === to_wallet);
        if (!registryRow) {
            await addToLedger({
                type: 'PURCHASE',
                from: from_wallet,
                to: sellerWallet || 'SYSTEM',
                amount: 0,
                token: 'RGT',
                extra: { product_id, product_type, reward_amount, note: 'No registry match — treated as free interest' },
                debit: false, credit: false,
                instruction: packet.instruction || null, packet_id: packet.packet_id || null
            });
            return { success: true, amount: 0, reward_amount: 0, token: 'RGT', note: 'Free interest registered' };
        }
        amount = registryRow.deductionAmount;
        reward_amount = registryRow.rewardAmount;
        token = registryRow.token;
        sellerWallet = registryRow.targetWallet;
    }
    if (amount <= 0) {
        await addToLedger({
            type: 'PURCHASE',
            from: from_wallet,
            to: sellerWallet || 'SYSTEM',
            amount: 0,
            token: token || 'RGT',
            extra: { product_id, product_type, reward_amount, note: 'Zero amount interest' },
            debit: false, credit: false,
            instruction: packet.instruction || null, packet_id: packet.packet_id || null
        });
        return { success: true, amount: 0, reward_amount: 0, token: token || 'RGT', note: 'Interest registered' };
    }

    if (reward_amount > 0) {
        const vaultBalance = await getBalance(WALLET_IDS.ADMIN, token);
        if (vaultBalance >= reward_amount) {
            await updateBalance(from_wallet, reward_amount, token);
            await updateBalance(WALLET_IDS.ADMIN, -reward_amount, token);
            if (io) io.emit('packet', { type: 'PURCHASE_REWARD', from: 'VAULT', to: from_wallet, amount: reward_amount, token, extra: { product_id }, timestamp: Date.now() });
        }
    }
    const deduction = await deductWithFallback(from_wallet, amount, token);
    if (!deduction.success) return { success: false, error: 'Insufficient balance', silent: true };
    if (sellerWallet && await walletExists(sellerWallet)) await updateBalance(sellerWallet, amount, deduction.token);

    await addToLedger({
        type: 'PURCHASE', from: from_wallet, to: sellerWallet, amount, token: deduction.token,
        extra: { product_id, product_type, reward_amount, reward_first: true, net_change: reward_amount - amount, token_used: deduction.token, preferred_token: token, silent: true },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true, amount, reward_amount, token: deduction.token };
}

async function handleProductInterest(packet) {
    await addToLedger({ type: 'PRODUCT_INTEREST', from: packet.from_wallet, to: packet.to_wallet || 'SYSTEM', amount: packet.amount || 0, token: packet.token || 'RGT', extra: { product_id: packet.product_id, product_type: packet.product_type }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleEventAttend(packet) {
    await addToLedger({ type: 'EVENT_ATTEND', from: packet.from_wallet, to: packet.to_wallet || 'EVENT_SYSTEM', amount: packet.amount || 0, token: packet.token || 'RGT', extra: { eventId: packet.eventId, eventTitle: packet.eventTitle }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleStreamReward(packet) {
    const { from_wallet, to_wallet, amount, token, listener_reward } = packet;
    if (from_wallet) { const guard = await guardActive(from_wallet); if (!guard.ok) return { success: false, error: guard.error }; }
    if (packet.play_percentage && packet.play_percentage < 30) return { success: false, error: 'Below 30%' };
    const cashBoxBalance = await getBalance(WALLET_IDS.CASH_BOX, token);
    const totalRequired = amount + (listener_reward || 0);
    if (cashBoxBalance < totalRequired) return { success: false, error: 'Insufficient CASH_BOX' };
    await updateBalance(WALLET_IDS.CASH_BOX, -totalRequired, token);
    if (amount > 0) await updateBalance(to_wallet, amount, token);
    if (listener_reward > 0 && from_wallet) await updateBalance(from_wallet, listener_reward, token);
    await addToLedger({ type: 'STREAM_REWARD', from: from_wallet, to: to_wallet, amount, token, extra: { media_title: packet.media_title, listener_reward }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleStreamRatesUpdated(packet) {
    if (!await isAdmin(packet.from_wallet)) return { success: false, error: 'Unauthorized' };
    await addToLedger({ type: 'STREAM_RATES_UPDATED', from: packet.from_wallet || 'ADMIN', to: 'SYSTEM', amount: 0, token: 'RGT', extra: packet.rates, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleStreamTick(packet) {
    const { box_code, delivered, total_delivered, token, slot } = packet;
    await dbRun(`UPDATE module_boxes SET delivered = ?, total_delivered = ?, updated_at = ? WHERE code = ?`, [delivered || 0, total_delivered || 0, Date.now(), box_code]);
    if (io) io.emit('packet', { type: 'STREAM_TICK', box_code, delivered, total_delivered, token, slot, timestamp: Date.now() });
    return { success: true };
}
async function handleFundDisbursement(packet) {
    const { from_wallet, source, token, amount, target_module } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    const sourceWallet = source === 'crown' ? WALLET_IDS.CROWN_BANK : WALLET_IDS.ADMIN;
    const sourceBalance = await getBalance(sourceWallet, token);
    if (sourceBalance < amount) return { success: false, error: 'Insufficient source' };
    await updateBalance(sourceWallet, -amount, token);
    await updateBalance(target_module || 'FIN107', amount, token);
    await addToLedger({ type: 'FUND_DISBURSEMENT', from: sourceWallet, to: target_module || 'FIN107', amount, token, extra: { source, target_module, purpose: packet.purpose || 'general' }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    if (io) io.emit('packet', { type: 'MODULE_FUND', from_wallet, to_module: target_module || 'FIN107', amount, token, source, timestamp: Date.now() });
    return { success: true };
}
async function handleModuleDisbursed(packet) {
    await addToLedger({ type: 'MODULE_DISBURSED', from: packet.from_wallet, to: packet.to_wallet || packet.to_module, amount: packet.amount || 0, token: packet.token || 'RGT', extra: { module: packet.to_module, source: packet.source, slot: packet.slot }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}

// ============================================
// MEDIA HANDLERS
// ============================================
async function handleRadioSwitch(packet) {
    if (!await isAdmin(packet.from_wallet) && packet.from_wallet !== 'ADMIN_VAULT') {
        return { success: false, error: 'Admin only' };
    }
    const extra = {
        track_hash: packet.track_hash || packet.mediaId || null,
        audio_url: packet.audio_url || (packet.track_hash ? `/media/${packet.track_hash}` : null),
        title: packet.title || 'Radio Show',
        artist: packet.artist || '',
        artist_wallet: packet.artist_wallet || null,
        artist_rate: packet.artist_rate || 0.5,
        listener_rate: packet.listener_rate || 0.05,
        token: packet.token || 'RGT',
        cover_url: packet.cover_url || null,
        started_at: Date.now()
    };
    await addToLedger({
        type: 'RADIO_SWITCH',
        from: packet.from_wallet, to: 'ALL',
        amount: 0, token: extra.token,
        extra, debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    console.log(`📻 Radio switched: ${extra.title}`);
    return { success: true, ...extra };
}

async function handleVideoShowStart(packet) {
    if (!await isAdmin(packet.from_wallet) && packet.from_wallet !== 'ADMIN_VAULT') {
        return { success: false, error: 'Admin only' };
    }
    const extra = {
        show_hash: packet.show_hash || packet.mediaId || null,
        video_url: packet.video_url || (packet.show_hash ? `/media/${packet.show_hash}` : null),
        title: packet.title || 'Live Show',
        artist: packet.artist || '',
        artist_wallet: packet.artist_wallet || null,
        artist_rate_per_min: packet.artist_rate_per_min || 0.1,
        listener_rate_per_min: packet.listener_rate_per_min || 0.01,
        token: packet.token || 'RGT',
        poster_url: packet.poster_url || null,
        started_at: Date.now()
    };
    await addToLedger({
        type: 'VIDEO_SHOW_START',
        from: packet.from_wallet, to: 'ALL',
        amount: 0, token: extra.token,
        extra, debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    console.log(`🎬 Video show started: ${extra.title}`);
    return { success: true, ...extra };
}

async function handleVideoShowEnd(packet) {
    if (!await isAdmin(packet.from_wallet) && packet.from_wallet !== 'ADMIN_VAULT') {
        return { success: false, error: 'Admin only' };
    }
    const extra = {
        show_hash: packet.show_hash || null,
        ended_at: Date.now()
    };
    await addToLedger({
        type: 'VIDEO_SHOW_END',
        from: packet.from_wallet, to: 'ALL',
        amount: 0, token: 'RGT',
        extra, debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    console.log(`🎬 Video show ended`);
    return { success: true };
}

async function handleShowTick(packet) {
    const { from_wallet, artist_wallet, amount, token, show_hash } = packet;
    const guard = await guardActive(from_wallet);
    if (!guard.ok) return { success: false, error: guard.error };
    const amt = parseFloat(amount) || 0;
    if (amt <= 0) return { success: false, error: 'Invalid amount' };

    const cashBox = await getBalance(WALLET_IDS.CASH_BOX, token || 'RGT');
    if (cashBox < amt) return { success: false, error: 'Insufficient CASH_BOX' };

    await updateBalance(WALLET_IDS.CASH_BOX, -amt, token || 'RGT');
    const artistCut = amt * 0.9;
    const listenerCut = amt * 0.1;
    if (artist_wallet) await updateBalance(artist_wallet, artistCut, token || 'RGT');
    await updateBalance(from_wallet, listenerCut, token || 'RGT');

    await addToLedger({
        type: 'SHOW_TICK',
        from: from_wallet, to: artist_wallet || 'SYSTEM',
        amount: amt, token: token || 'RGT',
        extra: { show_hash, artist_cut: artistCut, listener_cut: listenerCut },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true, artist_cut: artistCut, listener_cut: listenerCut };
}

// ============================================
// SHOW HANDLERS — Virtual Shows (v6.5 adds venue + check-in)
// ============================================
async function handleShowCreate(packet) {
    if (!await isAdmin(packet.from_wallet)) return { success: false, error: 'Admin only' };
    const {
        title, artist_wallet, artist_name, scheduled_at, duration_minutes,
        ticket_price, ticket_token, attendee_reward, reward_token,
        artist_rate_per_min, artist_stream_token,
        listener_rate_per_min, listener_stream_token,
        poster_url, video_url,
        venue, venue_address, check_in_opens_at, is_virtual
    } = packet;

    if (!title) return { success: false, error: 'Title required' };
    if (!artist_wallet) return { success: false, error: 'Artist wallet required' };
    if (!scheduled_at) return { success: false, error: 'Scheduled time required' };

    const showId = 'SHOW_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);
    const now = Date.now();
    const scheduledMs = parseInt(scheduled_at);
    // default: check-in opens 60 min before scheduled_at
    const checkInOpens = check_in_opens_at
        ? parseInt(check_in_opens_at)
        : (scheduledMs - 60 * 60 * 1000);

    await dbRun(
        `INSERT INTO shows (id, title, artist_wallet, artist_name, scheduled_at, duration_minutes,
         ticket_price, ticket_token, attendee_reward, reward_token,
         artist_rate_per_min, artist_stream_token, listener_rate_per_min, listener_stream_token,
         poster_url, video_url, status, is_virtual, venue, venue_address, check_in_opens_at,
         created_by, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'scheduled', ?, ?, ?, ?, ?, ?)`,
        [showId, title, artist_wallet, artist_name || '', scheduledMs, parseInt(duration_minutes) || 60,
         parseFloat(ticket_price) || 0, ticket_token || 'RGT',
         parseFloat(attendee_reward) || 0, reward_token || 'RGT',
         parseFloat(artist_rate_per_min) || 0.1, artist_stream_token || 'RGT',
         parseFloat(listener_rate_per_min) || 0.01, listener_stream_token || 'RGT',
         poster_url || '', video_url || '',
         is_virtual === 0 || is_virtual === false ? 0 : 1,
         venue || '', venue_address || '', checkInOpens,
         packet.from_wallet, now]
    );

    await addToLedger({
        type: 'SHOW_CREATED',
        from: packet.from_wallet, to: 'SYSTEM',
        amount: 0, token: 'RGT',
        extra: { show_id: showId, title, artist_wallet, scheduled_at: scheduledMs, venue: venue || '', venue_address: venue_address || '' },
        debit: false, credit: false,
        packet_id: packet.packet_id || null
    });

    return { success: true, show_id: showId, status: 'scheduled' };
}

async function handleShowAnnounce(packet) {
    if (!await isAdmin(packet.from_wallet)) return { success: false, error: 'Admin only' };
    const { show_id } = packet;
    if (!show_id) return { success: false, error: 'show_id required' };

    const show = await dbGet('SELECT * FROM shows WHERE id = ?', [show_id]);
    if (!show) return { success: false, error: 'Show not found' };
    if (show.status !== 'scheduled') return { success: false, error: 'Show cannot be announced in status: ' + show.status };

    await dbRun(`UPDATE shows SET status = 'announced', announced_at = ? WHERE id = ?`, [Date.now(), show_id]);

    if (io) {
        io.emit('packet', {
            type: 'SHOW_ANNOUNCED',
            from_wallet: packet.from_wallet,
            to_wallet: 'ALL',
            amount: 0,
            token: show.ticket_token,
            extra: {
                show_id: show.id,
                title: show.title,
                artist_wallet: show.artist_wallet,
                artist_name: show.artist_name,
                scheduled_at: show.scheduled_at,
                duration_minutes: show.duration_minutes,
                ticket_price: show.ticket_price,
                ticket_token: show.ticket_token,
                attendee_reward: show.attendee_reward,
                reward_token: show.reward_token,
                poster_url: show.poster_url,
                venue: show.venue || null,
                venue_address: show.venue_address || null,
                check_in_opens_at: show.check_in_opens_at || null
            },
            timestamp: Date.now()
        });
    }

    await addToLedger({
        type: 'SHOW_ANNOUNCED',
        from: packet.from_wallet, to: 'ALL',
        amount: 0, token: show.ticket_token,
        extra: { show_id, title: show.title, venue: show.venue || '' },
        debit: false, credit: false,
        packet_id: packet.packet_id || null
    });

    return { success: true, show_id, status: 'announced' };
}

async function handleShowStart(packet) {
    const isSystemCall = packet.from_wallet === 'SYSTEM';
    if (!isSystemCall && !await isAdmin(packet.from_wallet)) return { success: false, error: 'Admin only' };
    const { show_id } = packet;
    if (!show_id) return { success: false, error: 'show_id required' };

    const show = await dbGet('SELECT * FROM shows WHERE id = ?', [show_id]);
    if (!show) return { success: false, error: 'Show not found' };
    if (show.status === 'live') return { success: true, message: 'Already live' };
    if (show.status === 'ended') return { success: false, error: 'Show already ended' };

    await dbRun(`UPDATE shows SET status = 'live', started_at = ? WHERE id = ?`, [Date.now(), show_id]);

    if (io) {
        io.emit('packet', {
            type: 'VIDEO_SHOW_START',
            from_wallet: isSystemCall ? 'SYSTEM' : packet.from_wallet,
            to_wallet: 'ALL',
            amount: 0,
            token: show.artist_stream_token,
            extra: {
                show_id: show.id,
                show_hash: show.video_url,
                video_url: show.video_url,
                title: show.title,
                artist: show.artist_name,
                artist_wallet: show.artist_wallet,
                artist_rate_per_min: show.artist_rate_per_min,
                listener_rate_per_min: show.listener_rate_per_min,
                token: show.artist_stream_token,
                listener_token: show.listener_stream_token,
                poster_url: show.poster_url,
                duration_minutes: show.duration_minutes,
                venue: show.venue || null,
                venue_address: show.venue_address || null,
                started_at: Date.now(),
                auto_started: isSystemCall
            },
            timestamp: Date.now()
        });
    }

    await addToLedger({
        type: 'SHOW_STARTED',
        from: isSystemCall ? 'SYSTEM' : packet.from_wallet, to: 'ALL',
        amount: 0, token: show.artist_stream_token,
        extra: { show_id, title: show.title, auto_started: isSystemCall },
        debit: false, credit: false,
        packet_id: packet.packet_id || null
    });

    console.log(`🎬 Show started: ${show.title} (${isSystemCall ? 'auto' : 'manual'})`);
    return { success: true, show_id, status: 'live' };
}

async function handleShowEnd(packet) {
    const isSystemCall = packet.from_wallet === 'SYSTEM';
    if (!isSystemCall && !await isAdmin(packet.from_wallet)) return { success: false, error: 'Admin only' };
    const { show_id } = packet;
    if (!show_id) return { success: false, error: 'show_id required' };

    const show = await dbGet('SELECT * FROM shows WHERE id = ?', [show_id]);
    if (!show) return { success: false, error: 'Show not found' };
    if (show.status === 'ended') return { success: true, message: 'Already ended' };

    await dbRun(`UPDATE shows SET status = 'ended', ended_at = ? WHERE id = ?`, [Date.now(), show_id]);

    if (io) {
        io.emit('packet', {
            type: 'VIDEO_SHOW_END',
            from_wallet: isSystemCall ? 'SYSTEM' : packet.from_wallet,
            to_wallet: 'ALL',
            amount: 0,
            token: show.artist_stream_token,
            extra: { show_id: show.id, show_hash: show.video_url, ended_at: Date.now() },
            timestamp: Date.now()
        });
    }

    await addToLedger({
        type: 'SHOW_ENDED',
        from: isSystemCall ? 'SYSTEM' : packet.from_wallet, to: 'ALL',
        amount: 0, token: show.artist_stream_token,
        extra: { show_id, title: show.title },
        debit: false, credit: false,
        packet_id: packet.packet_id || null
    });

    console.log(`🎬 Show ended: ${show.title}`);
    return { success: true, show_id, status: 'ended' };
}

// ============================================
// SHOW CHECK-IN HANDLER (v6.5)
// ============================================
async function handleShowCheckin(packet) {
    const { show_id, from_wallet, ticket_id, attendee_wallet } = packet;
    if (!show_id) return { success: false, error: 'show_id required' };
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Staff only' };

    const show = await dbGet('SELECT * FROM shows WHERE id = ?', [show_id]);
    if (!show) return { success: false, error: 'Show not found' };
    if (show.status !== 'live' && show.status !== 'announced') {
        return { success: false, error: 'Entrance not open for this show' };
    }

    // Locate the attendance row by ticket_id OR wallet
    let attendance;
    if (ticket_id) {
        attendance = await dbGet('SELECT * FROM show_attendance WHERE show_id = ? AND ticket_id = ?', [show_id, ticket_id]);
    }
    if (!attendance && attendee_wallet) {
        attendance = await dbGet('SELECT * FROM show_attendance WHERE show_id = ? AND attendee_wallet = ?', [show_id, attendee_wallet]);
    }
    if (!attendance) return { success: false, error: 'Pass could not be verified', reason: 'not_found' };

    if (attendance.checked_in_at) {
        return {
            success: false,
            error: 'This pass was already used',
            reason: 'already_checked_in',
            checked_in_at: attendance.checked_in_at,
            checked_in_by: attendance.checked_in_by,
            attendee_wallet: attendance.attendee_wallet,
            ticket_id: attendance.ticket_id
        };
    }

    const now = Date.now();
    await dbRun(
        `UPDATE show_attendance SET checked_in_at = ?, checked_in_by = ?, admission_status = 'arrived' WHERE id = ?`,
        [now, from_wallet, attendance.id]
    );

    const member = await dbGet('SELECT name, username FROM members WHERE wallet = ?', [attendance.attendee_wallet]);

    if (io) {
        io.emit('packet', {
            type: 'SHOW_CHECKIN',
            from_wallet,
            to_wallet: attendance.attendee_wallet,
            amount: 0,
            token: attendance.ticket_token,
            extra: {
                show_id,
                show_title: show.title,
                attendee_wallet: attendance.attendee_wallet,
                attendee_name: member ? member.name : null,
                ticket_id: attendance.ticket_id,
                checked_in_at: now,
                checked_in_by: from_wallet
            },
            timestamp: now
        });
    }

    await addToLedger({
        type: 'SHOW_CHECKIN',
        from: from_wallet, to: attendance.attendee_wallet,
        amount: 0, token: attendance.ticket_token,
        extra: { show_id, show_title: show.title, ticket_id: attendance.ticket_id, checked_in_at: now },
        debit: false, credit: false,
        packet_id: packet.packet_id || null
    });

    return {
        success: true,
        welcome: true,
        attendee_wallet: attendance.attendee_wallet,
        attendee_name: member ? member.name : null,
        ticket_id: attendance.ticket_id,
        checked_in_at: now,
        checked_in_by: from_wallet,
        admission_status: 'arrived'
    };
}

// ============================================
// PACKET HANDLERS — FIN107 BOXES
// ============================================
async function handleModuleBoxCreated(packet) {
    const b = packet.box; if (!b) return { success: false, error: 'box required' };
    await dbRun(
        `INSERT OR REPLACE INTO module_boxes (id, code, name, wallet, source, token, slot, amount, mode, window_ms, status, delivered, total_delivered, sends, drip_started_at, next_fire_at, last_fire_at, history, created_at, updated_at, created_by)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [b.id, b.code, b.name, b.wallet, b.source || 'vault', b.token || null,
         b.slot || null, b.amount || 0, b.mode || null, b.window_ms || 0,
         b.status || 'not-programmed', 0, 0, 0, null, null, null,
         JSON.stringify([]), b.created_at || Date.now(), Date.now(), packet.from_wallet]
    );
    await addToLedger({ type: 'MODULE_BOX_CREATED', from: packet.from_wallet, to: 'FIN107', amount: 0, token: 'RGT', extra: { box: b }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleModuleBoxConfigured(packet) {
    const b = packet.box; if (!b) return { success: false, error: 'box required' };
    await dbRun(`UPDATE module_boxes SET source = ?, token = ?, slot = ?, amount = ?, mode = ?, window_ms = ?, status = ?, updated_at = ? WHERE code = ?`,
        [b.source, b.token, b.slot, b.amount, b.mode, b.window_ms || 0, b.status || 'ready', Date.now(), b.code]);
    await addToLedger({ type: 'MODULE_BOX_CONFIGURED', from: packet.from_wallet, to: 'FIN107', amount: 0, token: 'RGT', extra: { box: b }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleModuleBoxStarted(packet) {
    const b = packet.box; if (!b) return { success: false, error: 'box required' };
    await dbRun(`UPDATE module_boxes SET status = 'running', drip_started_at = ?, next_fire_at = ?, updated_at = ? WHERE code = ?`,
        [Date.now(), b.next_fire_at || null, Date.now(), b.code]);
    await addToLedger({ type: 'MODULE_BOX_STARTED', from: packet.from_wallet, to: b.code || 'FIN107', amount: 0, token: b.token || 'RGT', extra: { box: b }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleModuleBoxPaused(packet) {
    const b = packet.box; if (!b) return { success: false, error: 'box required' };
    await dbRun(`UPDATE module_boxes SET status = 'paused', updated_at = ? WHERE code = ?`, [Date.now(), b.code]);
    await addToLedger({ type: 'MODULE_BOX_PAUSED', from: packet.from_wallet, to: b.code || 'FIN107', amount: 0, token: 'RGT', extra: { box: b }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleModuleBoxCycleComplete(packet) {
    await addToLedger({ type: 'MODULE_BOX_CYCLE_COMPLETE', from: packet.from_wallet, to: packet.box_code || 'FIN107', amount: packet.amount || 0, token: packet.token || 'RGT', extra: { box_code: packet.box_code, cycle: packet.cycle, amount: packet.amount, token: packet.token, slot: packet.slot }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}

// ============================================
// PACKET HANDLERS — NFT
// ============================================
async function handleNFTMint(packet) {
    const data = packet.data || packet;
    const from_wallet = packet.from_wallet || 'ADMIN_VAULT';
    const artist_name = data.artist_name || data.artistName;
    const artist_wallet = data.artist_wallet || data.artistWallet;
    const total_shares = data.total_shares || data.totalShares;
    const price_per_share = data.price_per_share || data.pricePerShare;
    const token = data.token || data.tokenSymbol || 'RGT';
    const slot = data.slot;
    const monthly_return = data.monthly_return || data.monthlyReturn || 0;
    const share_per_unit = data.share_per_unit || data.sharePerUnit || null;
    const image_url = data.image_url || data.imageUrl || null;

    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    if (!artist_name) return { success: false, error: 'Artist name required' };
    if (!total_shares || total_shares <= 0) return { success: false, error: 'Invalid shares' };
    if (!price_per_share || price_per_share <= 0) return { success: false, error: 'Invalid price' };

    const nftId = packet.nft_id || 'NFT_' + Date.now();
    await dbRun(
        `INSERT INTO nfts (id, artist_name, artist_wallet, total_shares, shares_available, price_per_share, token, slot, monthly_return, share_per_unit, image_url, description, benefits, status, minted_by, minted_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [nftId, artist_name, artist_wallet, total_shares, total_shares, price_per_share,
         token, slot || 'SLOT' + Date.now().toString().slice(-6),
         monthly_return, share_per_unit, image_url,
         data.description || '', data.benefits || '', 'active', from_wallet, Date.now()]
    );
    await addToLedger({
        type: 'NFT_MINT', from: from_wallet, to: 'ALL',
        amount: total_shares * price_per_share, token,
        extra: { id: nftId, nft_id: nftId, artist_name, artist_wallet, title: artist_name + ' - Share Certificate',
                 total_shares, price_per_share, monthly_return, share_per_unit, token,
                 slot: slot || '', image_url: image_url || '', image: image_url || '',
                 description: data.description || '', benefits: data.benefits || '',
                 total_value: total_shares * price_per_share },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    if (io) {
        io.emit('packet', {
            type: 'SHARE_CERTIFICATE_ISSUED',
            from_wallet,
            to_wallet: 'ALL',
            amount: 0,
            token,
            payload: {
                artist: artist_wallet,
                title: artist_name,
                total_shares,
                price_per_share,
                token,
                description: data.description || '',
                image: image_url || ''
            },
            timestamp: Date.now()
        });
    }
    return { success: true, nft_id: nftId };
}
async function handleNFTSharePurchase(packet) {
    const { from_wallet, nft_id, shares } = packet;
    const guard = await guardActive(from_wallet);
    if (!guard.ok) return { success: false, error: guard.error };
    if (!shares || shares <= 0) return { success: false, error: 'Invalid shares' };
    const nft = await dbGet('SELECT * FROM nfts WHERE id = ? AND status = ?', [nft_id, 'active']);
    if (!nft) return { success: false, error: 'NFT not found' };
    if (shares > nft.shares_available) return { success: false, error: 'Insufficient shares' };
    const totalCost = shares * nft.price_per_share;
    const balance = await getBalance(from_wallet, nft.token);
    if (balance < totalCost) return { success: false, error: 'Insufficient balance' };
    await updateBalance(from_wallet, -totalCost, nft.token);
    if (await walletExists(nft.artist_wallet)) await updateBalance(nft.artist_wallet, totalCost, nft.token);
    await dbRun(`UPDATE nfts SET shares_available = ? WHERE id = ?`, [nft.shares_available - shares, nft_id]);
    await addToLedger({
        type: 'NFT_SHARE_PURCHASE', from: from_wallet, to: nft.artist_wallet, amount: totalCost, token: nft.token,
        extra: { nft_id, shares, artist_name: nft.artist_name }, debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true, shares, totalCost };
}

// ============================================
// PACKET HANDLERS — FEED & MESSAGES
// ============================================
async function handleFeedPost(packet) {
    const { from_wallet, message, image, category, post_id } = packet;
    if (!message && !image) return { success: false, error: 'Message or image required' };
    const postId = post_id || Date.now();
    await dbRun(`INSERT INTO feed_posts (id, from_wallet, message, image, category, timestamp, attendCount, wantCount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [postId, from_wallet || 'UNKNOWN', message || '', image || null, category || 'Client', Date.now(), 0, 0]);
    await addToLedger({
        type: 'FEED_POST', from: from_wallet || 'UNKNOWN', to: 'ALL', amount: 0, token: 'RGT',
        extra: { id: postId, message: message || '', image: image || '', category: category || 'Client', wallet: from_wallet, timestamp: Date.now(), attendCount: 0, wantCount: 0 },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true };
}
async function handleFeedInteraction(packet) {
    const { from_wallet, post_id, interaction } = packet;
    if (!post_id || !interaction) return { success: false, error: 'Missing fields' };
    const column = interaction === 'attend' ? 'attendCount' : 'wantCount';
    await dbRun(`UPDATE feed_posts SET ${column} = ${column} + 1 WHERE id = ?`, [post_id]);
    const post = await dbGet(`SELECT * FROM feed_posts WHERE id = ?`, [post_id]);
    if (!post) return { success: false, error: 'Post not found' };
    await addToLedger({
        type: 'FEED_INTERACTION', from: from_wallet, to: 'ALL', amount: 0, token: 'RGT',
        extra: { post_id, interaction, attendCount: post.attendCount, wantCount: post.wantCount },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true, attendCount: post.attendCount, wantCount: post.wantCount };
}
async function handleP2PMessage(packet) {
    const { from_wallet, to_wallet, body, image } = packet;
    if (!body || body.trim() === '') return { success: false, error: 'Message required' };
    await dbRun(`INSERT INTO messages (from_wallet, to_wallet, body, image, timestamp, read) VALUES (?, ?, ?, ?, ?, ?)`,
        [from_wallet, to_wallet, body, image || null, Date.now(), 0]);
    await addToLedger({ type: 'P2P_MSG', from: from_wallet, to: to_wallet, amount: 0, token: 'RGT', extra: { message: body, image: image || null }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleBroadcast(packet) {
    const { from_wallet, message, image, body } = packet;
    const msgBody = body || message || '';
    await dbRun(`INSERT INTO broadcasts (from_wallet, body, image, timestamp) VALUES (?, ?, ?, ?)`, [from_wallet, msgBody, image || null, Date.now()]);
    await addToLedger({ type: 'BROADCAST_MSG', from: from_wallet, to: 'ALL', amount: 0, token: 'RGT', extra: { message: msgBody, image: image || null }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}

// ============================================
// PACKET HANDLERS — CASHOUT
// ============================================
async function handleCashOut(packet) {
    const { from_wallet, to_wallet, amount, currency, name, email, bank, account, processor, reference } = packet;
    const guard = await guardActive(from_wallet);
    if (!guard.ok) return { success: false, error: guard.error };
    if (amount <= 0) return { success: false, error: 'Invalid amount' };
    const requestId = reference || 'CASHOUT_' + Date.now();
    await dbRun(`INSERT INTO pending_cashouts (id, wallet, amount, currency, bankDetails, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [requestId, from_wallet, amount, currency || 'NGN', JSON.stringify({ name, email, bank, account, processor }), 'pending', Date.now()]);
    await addToLedger({
        type: 'CASH_OUT', from: from_wallet, to: to_wallet || 'BANKING_SYSTEM',
        amount, token: currency || 'NGN',
        extra: { requestId, processor, name, email, bank, account, currency: currency || 'NGN' },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true, requestId };
}
async function handleCashoutApproved(packet) {
    const { from_wallet, to_wallet, amount, currency, reference, request_id } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    const id = request_id || reference;
    if (id) await dbRun(`UPDATE pending_cashouts SET status = 'approved', approved_by = ?, approved_at = ? WHERE id = ?`, [from_wallet, Date.now(), id]);
    await addToLedger({ type: 'CASHOUT_APPROVED', from: 'ADMIN', to: to_wallet, amount: amount || 0, token: currency || 'NGN', extra: { reference: id }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleCashoutRejected(packet) {
    const { from_wallet, to_wallet, amount, reason, reference, request_id } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    const id = request_id || reference;
    if (id) await dbRun(`UPDATE pending_cashouts SET status = 'rejected', approved_by = ?, approved_at = ? WHERE id = ?`, [from_wallet, Date.now(), id]);
    await addToLedger({ type: 'CASHOUT_REJECTED', from: 'ADMIN', to: to_wallet, amount: amount || 0, token: 'NGN', extra: { reference: id, reason: reason || 'Rejected' }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}

// ============================================
// CRYPTO MASS PAY
// ============================================
async function handleMassPayoutExternal(packet) {
    const { from_wallet, source = 'user', token = 'RGT', recipients } = packet;

    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    if (!recipients || !Array.isArray(recipients) || recipients.length === 0) {
        return { success: false, error: 'No recipients' };
    }
    if (recipients.length > 10) return { success: false, error: 'Max 10 recipients per batch' };
    if (!isValidToken(token)) return { success: false, error: 'Invalid token' };
    if (!['user', 'admin'].includes(source)) return { success: false, error: 'Invalid source' };

    const batchId = 'MASS_PAYOUT_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);
    const results = [];
    let succeeded = 0, failed = 0, totalTokenAmount = 0, totalNgnValue = 0;

    const prepared = [];
    for (const r of recipients) {
        if (!r.wallet) {
            results.push({ wallet: 'UNKNOWN', ok: false, error: 'Missing wallet ID' });
            failed++;
            continue;
        }
        const member = await dbGet(
            'SELECT wallet, name, username, status, crypto_wallet, crypto_network FROM members WHERE wallet = ?',
            [r.wallet]
        );
        if (!member) {
            results.push({ wallet: r.wallet, ok: false, error: 'Not in registry' });
            failed++;
            continue;
        }
        if (member.status === 'frozen' || member.status === 'blacklisted') {
            results.push({ wallet: r.wallet, name: member.name, ok: false, error: 'Recipient ' + member.status });
            failed++;
            continue;
        }
        if (!member.crypto_wallet) {
            results.push({ wallet: r.wallet, name: member.name, ok: false, error: 'No crypto wallet set' });
            failed++;
            continue;
        }
        const amount = parseFloat(r.amount);
        if (isNaN(amount) || amount <= 0) {
            results.push({ wallet: r.wallet, name: member.name, ok: false, error: 'Invalid amount' });
            failed++;
            continue;
        }
        const rate = await getExchangeRate(token);
        if (!rate) {
            results.push({ wallet: r.wallet, name: member.name, ok: false, error: 'No exchange rate' });
            failed++;
            continue;
        }
        if (source === 'user') {
            const bal = await getBalance(r.wallet, token);
            if (bal < amount) {
                results.push({ wallet: r.wallet, name: member.name, ok: false, error: 'Insufficient balance' });
                failed++;
                continue;
            }
        }
        prepared.push({
            member, amount, rate,
            ngnValue: amount * rate,
            cryptoWallet: member.crypto_wallet,
            cryptoNetwork: member.crypto_network
        });
    }

    if (source === 'admin' && prepared.length > 0) {
        const total = prepared.reduce((s, e) => s + e.amount, 0);
        const adminBal = await getBalance(WALLET_IDS.ADMIN, token);
        if (adminBal < total) {
            return { success: false, error: `Admin balance insufficient: ${adminBal} < ${total}`, batch_id: batchId };
        }
        await updateBalance(WALLET_IDS.ADMIN, -total, token);
    }

    for (const entry of prepared) {
        try {
            if (source === 'user') {
                await updateBalance(entry.member.wallet, -entry.amount, token);
            }

            const result = await quidaxBuyAndSend({
                ngnAmount: entry.ngnValue,
                destination: entry.cryptoWallet,
                network: entry.cryptoNetwork,
                asset: 'USDT'
            });

            if (!result.ok) {
                if (source === 'user') {
                    await updateBalance(entry.member.wallet, entry.amount, token);
                }
                results.push({ wallet: entry.member.wallet, name: entry.member.name, ok: false, error: result.error });
                failed++;
                continue;
            }

            const slipId = 'SLIP-' + Date.now() + '-' + Math.random().toString(36).substr(2, 6);
            await dbRun(
                `INSERT INTO settlement_slips (slip_id, wallet, token, token_amount, rate, ngn_value, bank_details, crypto_wallet, crypto_network, status, quidax_tx_hash, quidax_reference, quidax_order_id, usdt_amount, completed_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?, ?, ?, ?)`,
                [slipId, entry.member.wallet, token, entry.amount, entry.rate, entry.ngnValue,
                 JSON.stringify({ batch_id: batchId, source }),
                 entry.cryptoWallet, entry.cryptoNetwork,
                 result.tx_hash, result.reference, result.order_id, result.received, Date.now()]
            );

            await addToLedger({
                type: 'CASHOUT_COMPLETED',
                from: entry.member.wallet,
                to: entry.cryptoWallet,
                amount: entry.amount,
                token,
                extra: {
                    batch_id: batchId, slip_id: slipId,
                    name: entry.member.name || '', username: entry.member.username || '',
                    rate: entry.rate, ngn_value: entry.ngnValue,
                    usdt_amount: result.received, crypto_network: entry.cryptoNetwork,
                    quidax_tx_hash: result.tx_hash, simulated: result.simulated || false,
                    source, event: 'MASS_PAYOUT_ITEM'
                },
                debit: false, credit: false,
                packet_id: packet.packet_id ? packet.packet_id + '_' + entry.member.wallet : null
            });

            totalTokenAmount += entry.amount;
            totalNgnValue += entry.ngnValue;
            succeeded++;
            results.push({
                wallet: entry.member.wallet, name: entry.member.name,
                ok: true, amount: entry.amount, ngn_value: entry.ngnValue,
                received_usdt: result.received, tx_hash: result.tx_hash,
                destination: entry.cryptoWallet, network: entry.cryptoNetwork,
                simulated: result.simulated || false, slip_id: slipId
            });
        } catch (err) {
            if (source === 'user') {
                try { await updateBalance(entry.member.wallet, entry.amount, token); } catch (e) {}
            }
            results.push({ wallet: entry.member.wallet, name: entry.member.name, ok: false, error: err.message });
            failed++;
        }
    }

    await addToLedger({
        type: 'MASS_PAYOUT_EXTERNAL',
        from: from_wallet, to: 'MULTIPLE',
        amount: totalTokenAmount, token,
        extra: {
            batch_id: batchId, source,
            count: recipients.length, succeeded, failed,
            total_token: totalTokenAmount, total_ngn: totalNgnValue,
            results: results.map(r => ({
                wallet: r.wallet, name: r.name || null, ok: r.ok,
                amount: r.amount || null, tx_hash: r.tx_hash || null, error: r.error || null
            }))
        },
        debit: false, credit: false,
        packet_id: packet.packet_id || null
    });

    if (io) {
        io.emit('packet', {
            type: 'MASS_PAYOUT_EXTERNAL_COMPLETE',
            batch_id: batchId, source, total: recipients.length, succeeded, failed,
            timestamp: Date.now()
        });
    }

    return {
        success: true, batch_id: batchId, source,
        total: recipients.length, succeeded, failed,
        total_token: totalTokenAmount, total_ngn: totalNgnValue,
        results
    };
}

// ============================================
// PACKET HANDLERS — VOUCHERS
// ============================================
async function handleVoucherGenerate(packet) {
    const { from_wallet, code, expires_at, max_uses, to_wallet } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    if (!code) return { success: false, error: 'Code required' };
    const expiresAt = expires_at || Date.now() + 90 * 24 * 60 * 60 * 1000;
    await dbRun(`INSERT OR REPLACE INTO vouchers (code, amount, token, expires_at, max_uses, used_count, created_by, created_at, active, to_wallet) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [code.toUpperCase(), 0, 'REGISTRATION', expiresAt, max_uses || 1, 0, from_wallet, Date.now(), 1, to_wallet || null]);
    await addToLedger({
        type: 'VOUCHER_GENERATE', from: from_wallet, to: to_wallet || 'SYSTEM', amount: 0, token: 'REGISTRATION',
        extra: { code: code.toUpperCase(), seat_number: packet.seat_number, category: packet.category, voucher_type: packet.voucher_type || 'registration_proof', expires_at: expiresAt, max_uses: max_uses || 1, used_count: 0, active: 1, created_by: from_wallet, created_at: Date.now(), to_wallet: to_wallet || null },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true, code: code.toUpperCase() };
}
async function handleVoucherRedeem(packet) {
    const { from_wallet, code } = packet;
    const voucher = await dbGet('SELECT * FROM vouchers WHERE code = ? AND active = 1', [code.toUpperCase()]);
    if (!voucher) return { success: false, error: 'Voucher not found' };
    if (voucher.used_count >= voucher.max_uses) return { success: false, error: 'Voucher exhausted' };
    if (voucher.expires_at < Date.now()) return { success: false, error: 'Voucher expired' };
    await dbRun(`UPDATE vouchers SET used_count = ? WHERE code = ?`, [voucher.used_count + 1, code.toUpperCase()]);
    await addToLedger({
        type: 'VOUCHER_REDEEMED', from: from_wallet, to: voucher.created_by, amount: 0, token: 'REGISTRATION',
        extra: { code: code.toUpperCase(), redeemed_by: from_wallet, redeemed_at: Date.now(), used_count: voucher.used_count + 1 },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true };
}

// ============================================
// PACKET HANDLERS — MEMBER STATUS
// ============================================
async function handleFreeze(packet) {
    if (!await isAdmin(packet.from_wallet)) return { success: false, error: 'Unauthorized' };
    await dbRun(`UPDATE members SET status = 'frozen' WHERE wallet = ?`, [packet.to_wallet]);
    await addToLedger({ type: 'FREEZE', from: packet.from_wallet, to: packet.to_wallet, amount: 0, token: 'RGT', debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleUnfreeze(packet) {
    if (!await isAdmin(packet.from_wallet)) return { success: false, error: 'Unauthorized' };
    await dbRun(`UPDATE members SET status = 'active' WHERE wallet = ?`, [packet.to_wallet]);
    await addToLedger({ type: 'UNFREEZE', from: packet.from_wallet, to: packet.to_wallet, amount: 0, token: 'RGT', debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleBlacklist(packet) {
    if (!await isAdmin(packet.from_wallet)) return { success: false, error: 'Unauthorized' };
    await dbRun(`UPDATE members SET status = 'blacklisted' WHERE wallet = ?`, [packet.to_wallet]);
    await addToLedger({ type: 'BLACKLIST', from: packet.from_wallet, to: packet.to_wallet, amount: 0, token: 'RGT', debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleUnblacklist(packet) {
    if (!await isAdmin(packet.from_wallet)) return { success: false, error: 'Unauthorized' };
    await dbRun(`UPDATE members SET status = 'active' WHERE wallet = ?`, [packet.to_wallet]);
    await addToLedger({ type: 'UNBLACKLIST', from: packet.from_wallet, to: packet.to_wallet, amount: 0, token: 'RGT', debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}

// ============================================
// PACKET HANDLERS — CRYPTO WALLET
// ============================================
async function handleCryptoWalletSet(packet) {
    const { to_wallet, crypto_wallet, crypto_network } = packet;
    if (!to_wallet) return { success: false, error: 'Member wallet required' };
    if (!crypto_wallet) return { success: false, error: 'Crypto wallet required' };
    const rules = { trc20: /^T[a-zA-Z0-9]{33}$/, bep20: /^0x[a-fA-F0-9]{40}$/, celo: /^0x[a-fA-F0-9]{40}$/ };
    if (crypto_network && rules[crypto_network] && !rules[crypto_network].test(crypto_wallet)) return { success: false, error: 'Address does not match network' };
    const existing = await dbGet('SELECT crypto_wallet FROM members WHERE wallet = ?', [to_wallet]);
    const isUpdate = existing && existing.crypto_wallet;
    await addToLedger({
        type: isUpdate ? 'CRYPTO_WALLET_UPDATED' : 'CRYPTO_WALLET_SET',
        from: packet.from_wallet || 'ADMIN', to: to_wallet, amount: 0, token: 'RGT',
        extra: { crypto_wallet, crypto_network: crypto_network || '', previous_wallet: isUpdate ? existing.crypto_wallet : null },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true, updated: !!isUpdate };
}

// ============================================
// PACKET HANDLERS — NOTES
// ============================================
async function handleNoteCreate(packet) {
    const { from_wallet, title, content, noteId } = packet;
    if (!content) return { success: false, error: 'Content required' };
    const id = noteId || 'NOTE_' + Date.now();
    await dbRun(`INSERT OR REPLACE INTO notes (id, wallet, title, content, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
        [id, from_wallet, title || content.substring(0, 40), content, Date.now(), Date.now()]);
    await addToLedger({ type: 'NEURAL_NOTE_CREATED', from: from_wallet, to: 'NOTEBOOK', amount: 0, token: 'RGT', extra: { title, content, noteId: id }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true, noteId: id };
}
async function handleNoteDelete(packet) {
    const { from_wallet, note_id, noteId } = packet;
    const id = note_id || noteId;
    await dbRun(`DELETE FROM notes WHERE id = ?`, [id]);
    await addToLedger({ type: 'NEURAL_NOTE_DELETED', from: from_wallet, to: 'NOTEBOOK', amount: 0, token: 'RGT', extra: { noteId: id }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleNotesCleared(packet) {
    await dbRun(`DELETE FROM notes WHERE wallet = ?`, [packet.from_wallet]);
    await addToLedger({ type: 'NEURAL_NOTES_CLEARED', from: packet.from_wallet, to: 'NOTEBOOK', amount: 0, token: 'RGT', debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}

// ============================================
// PACKET HANDLERS — REGISTRIES
// ============================================
async function handleWorkRegister(packet) {
    const { work_type, title, creator_wallet, creator_name } = packet;
    if (!work_type || !title || !creator_wallet) return { success: false, error: 'Missing fields' };
    const validTypes = ['song', 'film', 'book', 'play'];
    if (!validTypes.includes(work_type)) return { success: false, error: 'Invalid type' };
    const workId = 'WORK_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);
    const prefix = work_type.toUpperCase().slice(0, 3);
    const catalogId = 'RC-' + prefix + '-' + String(Date.now()).slice(-8);
    await dbRun(
        `INSERT INTO creative_works (id, catalog_id, work_type, title, creator_wallet, creator_name, co_creators, description, genre, language, duration, pages, release_date, isrc, isbn, imdb_id, script_id, file_hash, file_url, cover_url, status, registered_by, registered_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [workId, catalogId, work_type, title, creator_wallet, creator_name || '',
         JSON.stringify(packet.co_creators || []), packet.description || '', packet.genre || '',
         packet.language || '', packet.duration || 0, packet.pages || 0, packet.release_date || '',
         packet.isrc || '', packet.isbn || '', packet.imdb_id || '', packet.script_id || '',
         packet.file_hash || '', packet.file_url || '', packet.cover_url || '',
         'registered', packet.from_wallet, Date.now()]
    );
    await addToLedger({
        type: 'WORK_REGISTER', from: packet.from_wallet, to: 'CREATIVE_REGISTRY', amount: 0, token: 'RGT',
        extra: { work_id: workId, catalog_id: catalogId, work_type, title, creator_wallet, creator_name, co_creators: packet.co_creators || [], genre: packet.genre || '' },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true, work_id: workId, catalog_id: catalogId, work_type };
}
async function handleMerchRegister(packet) {
    const { merch_type, title, creator_wallet, creator_name } = packet;
    if (!merch_type || !title || !creator_wallet) return { success: false, error: 'Missing fields' };
    const validTypes = ['apparel', 'accessory', 'print', 'digital', 'collectible', 'other'];
    if (!validTypes.includes(merch_type)) return { success: false, error: 'Invalid type' };
    const merchId = 'MERCH_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);
    const catalogId = 'RC-MRC-' + String(Date.now()).slice(-8);
    await dbRun(
        `INSERT INTO merch_registry (id, catalog_id, merch_type, title, creator_wallet, creator_name, description, category, linked_work_id, price, token, stock, sizes, colors, materials, sku, image_url, image_urls, status, registered_by, registered_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [merchId, catalogId, merch_type, title, creator_wallet, creator_name || '',
         packet.description || '', packet.category || '', packet.linked_work_id || null,
         packet.price || 0, packet.token || 'RGT', packet.stock || 0,
         JSON.stringify(packet.sizes || []), JSON.stringify(packet.colors || []),
         packet.materials || '', packet.sku || '', packet.image_url || null,
         JSON.stringify(packet.image_urls || []), 'registered', packet.from_wallet, Date.now()]
    );
    await addToLedger({
        type: 'MERCH_REGISTER', from: packet.from_wallet, to: 'MERCH_REGISTRY', amount: 0, token: 'RGT',
        extra: { merch_id: merchId, catalog_id: catalogId, merch_type, title, creator_wallet, creator_name, linked_work_id: packet.linked_work_id || null, price: packet.price || 0, token: packet.token || 'RGT', stock: packet.stock || 0 },
        debit: false, credit: false,
        instruction: packet.instruction || null, packet_id: packet.packet_id || null
    });
    return { success: true, merch_id: merchId, catalog_id: catalogId, merch_type };
}

// ============================================
// PACKET HANDLERS — BOTS
// ============================================
async function handleFarmingPenalty(packet) {
    const { from_wallet, to_wallet, amount, destination } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    await updateBalance(to_wallet, -amount, 'RGT');
    if (destination === 'vault') await updateBalance(WALLET_IDS.ADMIN, amount, 'RGT');
    else await updateBalance(WALLET_IDS.CROWN_BANK, amount, 'RGT');
    await dbRun(`INSERT INTO penalty_vault (wallet, amount, reason, date) VALUES (?, ?, ?, ?)`, [to_wallet, amount, 'Farming abuse', Date.now()]);
    await addToLedger({ type: 'FARMING_PENALTY', from: to_wallet, to: destination || 'VAULT', amount, token: 'RGT', debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true };
}
async function handleSubscriptionBot(packet) {
    const { from_wallet, extracted_list, users_processed, total_extracted } = packet;
    if (!await isAdmin(from_wallet) && from_wallet !== 'SYSTEM') return { success: false, error: 'Unauthorized' };
    let count = 0, total = 0;
    if (Array.isArray(extracted_list)) {
        for (const entry of extracted_list) {
            if (!entry.wallet || !entry.amount) continue;
            await updateBalance(entry.wallet, -entry.amount, entry.token || 'RGT');
            count++; total += entry.amount;
        }
    }
    await addToLedger({ type: 'SUBSCRIPTION_BOT', from: from_wallet, to: 'SYSTEM', amount: total || total_extracted || 0, token: 'RGT', extra: { count: count || users_processed || 0, users: extracted_list || [] }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true, processed: count || users_processed, total };
}
async function handleFarmingBot(packet) {
    const { from_wallet, penalized_list, penalties_applied, total_penalty } = packet;
    if (!await isAdmin(from_wallet) && from_wallet !== 'SYSTEM') return { success: false, error: 'Unauthorized' };
    let count = 0, total = 0;
    if (Array.isArray(penalized_list)) {
        for (const entry of penalized_list) {
            if (!entry.wallet || !entry.amount) continue;
            await updateBalance(entry.wallet, -entry.amount, entry.token || 'RGT');
            await dbRun(`INSERT INTO penalty_vault (wallet, amount, reason, date) VALUES (?, ?, ?, ?)`, [entry.wallet, entry.amount, 'Farming abuse', Date.now()]);
            count++; total += entry.amount;
        }
    }
    await addToLedger({ type: 'FARMING_BOT', from: from_wallet, to: 'SYSTEM', amount: total || total_penalty || 0, token: 'RGT', extra: { count: count || penalties_applied || 0, penalized: penalized_list || [] }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true, processed: count || penalties_applied, total };
}
async function handleSuspiciousBot(packet) {
    const { from_wallet, penalized_list, penalties_applied, total_penalty } = packet;
    if (!await isAdmin(from_wallet) && from_wallet !== 'SYSTEM') return { success: false, error: 'Unauthorized' };
    let count = 0, total = 0;
    if (Array.isArray(penalized_list)) {
        for (const entry of penalized_list) {
            if (!entry.wallet || !entry.amount) continue;
            await updateBalance(entry.wallet, -entry.amount, entry.token || 'RGT');
            await dbRun(`INSERT INTO penalty_vault (wallet, amount, reason, date) VALUES (?, ?, ?, ?)`, [entry.wallet, entry.amount, 'Suspicious', Date.now()]);
            count++; total += entry.amount;
        }
    }
    await addToLedger({ type: 'SUSPICIOUS_BOT', from: from_wallet, to: 'SYSTEM', amount: total || total_penalty || 0, token: 'RGT', extra: { count: count || penalties_applied || 0, penalized: penalized_list || [] }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true, processed: count || penalties_applied, total };
}
async function handleMassCollect(packet) {
    const { from_wallet, collected_list, users_affected, total_collected } = packet;
    if (!await isAdmin(from_wallet) && from_wallet !== 'SYSTEM') return { success: false, error: 'Unauthorized' };
    let count = 0, total = 0;
    if (Array.isArray(collected_list)) {
        for (const entry of collected_list) {
            if (!entry.wallet || !entry.amount) continue;
            await updateBalance(entry.wallet, -entry.amount, entry.token || 'RGT');
            await updateBalance(WALLET_IDS.ADMIN, entry.amount, entry.token || 'RGT');
            count++; total += entry.amount;
        }
    }
    await addToLedger({ type: 'MASS_COLLECT', from: from_wallet, to: 'VAULT', amount: total || total_collected || 0, token: 'RGT', extra: { count: count || users_affected || 0, collected: collected_list || [] }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
    return { success: true, processed: count || users_affected, total };
}

// ============================================
// HYBRID INSTRUCTION EXECUTOR
// ============================================
async function executeHybridFunction(fnName, args, packet) {
    try {
        switch (fnName) {
            case 'transfer': return await handleTransfer({ from_wallet: args[0] || packet.from_wallet, to_wallet: args[1] || packet.to_wallet, amount: args[2] || packet.amount, token: args[3] || packet.token, instruction: packet.instruction, packet_id: packet.packet_id });
            case 'swap': return await handleSwap({ from_wallet: args[0] || packet.from_wallet, from_token: args[1] || packet.from_token, to_token: args[2] || packet.to_token, amount_a: args[3] || packet.amount_a, amount_b: args[4] || packet.amount_b, instruction: packet.instruction, packet_id: packet.packet_id });
            case 'massPay': return await handleMassPay({ from_wallet: packet.from_wallet, token: args[0], recipients: args[2] || [], source: args[1], instruction: packet.instruction, packet_id: packet.packet_id });
            case 'massPayoutExternal': return await handleMassPayoutExternal({ from_wallet: args[0] || packet.from_wallet, source: args[1] || packet.source || 'user', token: args[2] || packet.token || 'RGT', recipients: args[3] || packet.recipients || [], instruction: packet.instruction, packet_id: packet.packet_id });
            case 'cashOut': return await handleCashOut({ ...packet, ...(packet.data || {}) });
            case 'peerTransfer': return await handlePeerTransfer({ from_wallet: args[0] || packet.from_wallet, to_wallet: args[1] || packet.to_wallet, amount: args[2] || packet.amount, token: args[3] || packet.token, instruction: packet.instruction, packet_id: packet.packet_id });
            case 'startDisbursement': return await handleModuleBoxStarted({ ...packet, box: { code: args[0], source: args[1], token: args[2], slot: args[3], amount: args[4], mode: args[5], window_ms: args[6] } });
            case 'fundDisbursement': return await handleFundDisbursement({ from_wallet: packet.from_wallet, source: args[0], token: args[1], amount: args[2], target_module: args[3], purpose: args[4], instruction: packet.instruction, packet_id: packet.packet_id });
            case 'mintNFT': return await handleNFTMint({ ...packet, data: { artist_name: args[0], artist_wallet: args[1], total_shares: args[2], price_per_share: args[3], token: args[4] } });
            case 'purchaseShares': return await handleNFTSharePurchase({ from_wallet: args[0] || packet.from_wallet, nft_id: args[1] || packet.nft_id, shares: args[2] || packet.shares, instruction: packet.instruction, packet_id: packet.packet_id });
            case 'streamReward': return await handleStreamReward({ from_wallet: args[0] || packet.from_wallet, to_wallet: args[1] || packet.to_wallet, amount: args[2] || packet.amount, token: args[3] || packet.token, media_title: args[4] || packet.media_title, instruction: packet.instruction, packet_id: packet.packet_id });
            case 'attendEvent': return await handleEventAttend({ from_wallet: args[0] || packet.from_wallet, eventId: args[1] || packet.eventId, amount: args[2] || packet.amount, token: args[3] || packet.token, instruction: packet.instruction, packet_id: packet.packet_id });
            case 'expressInterest': return await handleProductInterest({ from_wallet: args[0] || packet.from_wallet, to_wallet: args[1] || packet.to_wallet, amount: args[2] || packet.amount, token: args[3] || packet.token, instruction: packet.instruction, packet_id: packet.packet_id });
            case 'registerUser': return await handleRegistrationRequest({ ...packet, name: args[0], username: args[1], role: args[2], tier: args[3], voucher: args[4] });
            case 'freezeWallet': return await handleFreeze({ from_wallet: args[0] || packet.from_wallet, to_wallet: args[1] || packet.to_wallet });
            case 'unfreezeWallet': return await handleUnfreeze({ from_wallet: args[0] || packet.from_wallet, to_wallet: args[1] || packet.to_wallet });
            case 'blacklistWallet': return await handleBlacklist({ from_wallet: args[0] || packet.from_wallet, to_wallet: args[1] || packet.to_wallet });
            case 'unblacklistWallet': return await handleUnblacklist({ from_wallet: args[0] || packet.from_wallet, to_wallet: args[1] || packet.to_wallet });
            case 'broadcast': return await handleBroadcast({ from_wallet: args[0] || packet.from_wallet, body: args[1] || packet.body, instruction: packet.instruction, packet_id: packet.packet_id });
            case 'radioSwitch': return await handleRadioSwitch({ ...packet, ...packet.data });
            case 'videoShowStart': return await handleVideoShowStart({ ...packet, ...packet.data });
            case 'videoShowEnd': return await handleVideoShowEnd({ ...packet, ...packet.data });
            case 'showCreate': return await handleShowCreate({ ...packet, ...packet.data });
            case 'showAnnounce': return await handleShowAnnounce({ ...packet, ...packet.data });
            case 'showStart': return await handleShowStart({ ...packet, ...packet.data });
            case 'showEnd': return await handleShowEnd({ ...packet, ...packet.data });
            case 'showCheckin': return await handleShowCheckin({ ...packet, ...packet.data });
            default:
                console.log(`⚠️ Unknown Solidity function: ${fnName}`);
                return { success: false, error: 'Unknown function: ' + fnName };
        }
    } catch (err) {
        return { success: false, error: err.message };
    }
}

// ============================================
// ROUTER
// ============================================
async function routePacket(packet) {
    try {
        const idem = await checkIdempotency(packet);
        if (!idem.ok) return { success: false, error: idem.error, duplicate: true };

        const check = await validatePacket(packet);
        if (!check.valid) {
            await recordProcessedPacket(packet, { success: false, error: check.error });
            return { success: false, error: check.error };
        }

        const rl = checkRateLimit(packet.from_wallet);
        if (!rl.ok) {
            await recordProcessedPacket(packet, { success: false, error: rl.error });
            return { success: false, error: rl.error, rateLimited: true };
        }

        if (!packet.from_wallet && packet.type !== 'REGISTRATION_REQUEST') packet.from_wallet = 'UNKNOWN';

        let result;

        if (packet.instruction && packet.instruction.function) {
            console.log(`[NeuralChain] Solidity: ${packet.instruction.function}`);
            result = await executeHybridFunction(packet.instruction.function, packet.instruction.args || [], packet);
            if (result && result.success) {
                await recordProcessedPacket(packet, result);
                return result;
            }
            console.log(`[NeuralChain] Solidity failed, fallback to type: ${packet.type}`);
        }

        const type = packet.type.toUpperCase();
        switch (type) {
            case 'REGISTRATION_REQUEST':      result = await handleRegistrationRequest(packet); break;
            case 'USER_REGISTERED':           result = await handleUserRegistration(packet); break;
            case 'WALLET_DELETED':            result = await handleWalletDeleted(packet); break;
            case 'RATES_UPDATED':             result = await handleRatesUpdated(packet); break;
            case 'TRANSFER':
            case 'VAULT_SEND':
            case 'VAULT_RECEIVE':             result = await handleTransfer(packet); break;
            case 'P2P_TRANSFER':
            case 'PEER_SETTLEMENT':           result = await handlePeerTransfer(packet); break;
            case 'MASS_PAY':                  result = await handleMassPay(packet); break;
            case 'MASS_PAY_BATCH':            result = await handleMassPayBatch(packet); break;
            case 'SWAP':                      result = await handleSwap(packet); break;
            case 'PURCHASE':                  result = await handlePurchase(packet); break;
            case 'PRODUCT_INTEREST':          result = await handleProductInterest(packet); break;
            case 'EVENT_ATTEND':              result = await handleEventAttend(packet); break;
            case 'STREAM_REWARD':             result = await handleStreamReward(packet); break;
            case 'STREAM_RATES_UPDATED':      result = await handleStreamRatesUpdated(packet); break;
            case 'STREAM_TICK':               result = await handleStreamTick(packet); break;
            case 'FUND_DISBURSEMENT':         result = await handleFundDisbursement(packet); break;
            case 'MODULE_DISBURSED':          result = await handleModuleDisbursed(packet); break;
            case 'MODULE_BOX_CREATED':        result = await handleModuleBoxCreated(packet); break;
            case 'MODULE_BOX_CONFIGURED':     result = await handleModuleBoxConfigured(packet); break;
            case 'MODULE_BOX_STARTED':        result = await handleModuleBoxStarted(packet); break;
            case 'MODULE_BOX_PAUSED':         result = await handleModuleBoxPaused(packet); break;
            case 'MODULE_BOX_TICK':           result = await handleStreamTick(packet); break;
            case 'MODULE_BOX_CYCLE_COMPLETE': result = await handleModuleBoxCycleComplete(packet); break;
            case 'BROADCAST_MSG':             result = await handleBroadcast(packet); break;
            case 'P2P_MSG':                   result = await handleP2PMessage(packet); break;
            case 'FEED_POST':                 result = await handleFeedPost(packet); break;
            case 'FEED_INTERACTION':          result = await handleFeedInteraction(packet); break;
            case 'NFT_MINT':                  result = await handleNFTMint(packet); break;
            case 'SHARE_CERTIFICATE_ISSUED':  result = await handleNFTMint(packet); break;
            case 'NFT_PURCHASE':
            case 'NFT_SHARE_PURCHASE':
            case 'SHARE_PURCHASE':            result = await handleNFTSharePurchase(packet); break;
            case 'CASH_OUT':
            case 'CASHOUT_REQUEST':           result = await handleCashOut(packet); break;
            case 'CASHOUT_APPROVED':          result = await handleCashoutApproved(packet); break;
            case 'CASHOUT_REJECTED':          result = await handleCashoutRejected(packet); break;
            case 'CASHOUT_SETTLEMENT':
                result = await (async () => {
                    const slip = await generateSettlementSlip(packet.from_wallet, packet.token, packet.amount, packet.bankDetails || {});
                    if (!slip.ok) return slip;
                    return await processSettlementSlip(slip.slip_id);
                })();
                break;
            case 'MASS_PAYOUT_EXTERNAL':      result = await handleMassPayoutExternal(packet); break;
            case 'SHOW_CREATE':               result = await handleShowCreate(packet); break;
            case 'SHOW_ANNOUNCE':             result = await handleShowAnnounce(packet); break;
            case 'SHOW_START':                result = await handleShowStart(packet); break;
            case 'SHOW_END':                  result = await handleShowEnd(packet); break;
            case 'SHOW_CHECKIN':              result = await handleShowCheckin(packet); break;
            case 'RADIO_SWITCH':              result = await handleRadioSwitch(packet); break;
            case 'VIDEO_SHOW_START':          result = await handleVideoShowStart(packet); break;
            case 'VIDEO_SHOW_END':            result = await handleVideoShowEnd(packet); break;
            case 'SHOW_TICK':                 result = await handleShowTick(packet); break;
            case 'EXCHANGE_RATE_UPDATE':
                result = await (async () => {
                    if (!await isAdmin(packet.from_wallet)) return { success: false, error: 'Admin only' };
                    const r = await updateExchangeRate(packet.token, packet.rate_ngn, packet.currency, packet.from_wallet);
                    if (io) io.emit('packet', { type: 'EXCHANGE_RATE_UPDATED', token: packet.token, rate_ngn: packet.rate_ngn, currency: packet.currency || 'NGN', updated_by: packet.from_wallet, timestamp: Date.now() });
                    await addToLedger({ type: 'EXCHANGE_RATE_UPDATED', from: packet.from_wallet, to: 'SYSTEM', amount: 0, token: packet.token, extra: { token: packet.token, rate_ngn: packet.rate_ngn }, debit: false, credit: false, instruction: packet.instruction || null, packet_id: packet.packet_id || null });
                    return { success: true, ...r };
                })();
                break;
            case 'CRYPTO_WALLET_SET':
            case 'CRYPTO_WALLET_UPDATED':     result = await handleCryptoWalletSet(packet); break;
            case 'VOUCHER_GENERATE':          result = await handleVoucherGenerate(packet); break;
            case 'VOUCHER_REDEEM':            result = await handleVoucherRedeem(packet); break;
            case 'FREEZE':                    result = await handleFreeze(packet); break;
            case 'UNFREEZE':                  result = await handleUnfreeze(packet); break;
            case 'BLACKLIST':                 result = await handleBlacklist(packet); break;
            case 'UNBLACKLIST':               result = await handleUnblacklist(packet); break;
            case 'FARMING_PENALTY':           result = await handleFarmingPenalty(packet); break;
            case 'SUBSCRIPTION_BOT':          result = await handleSubscriptionBot(packet); break;
            case 'FARMING_BOT':               result = await handleFarmingBot(packet); break;
            case 'SUSPICIOUS_BOT':            result = await handleSuspiciousBot(packet); break;
            case 'MASS_COLLECT':              result = await handleMassCollect(packet); break;
            case 'NEURAL_NOTE_CREATED':
            case 'NEURAL_NOTE_UPDATED':       result = await handleNoteCreate(packet); break;
            case 'NEURAL_NOTE_DELETED':       result = await handleNoteDelete(packet); break;
            case 'NEURAL_NOTES_CLEARED':      result = await handleNotesCleared(packet); break;
            case 'WORK_REGISTER':             result = await handleWorkRegister(packet); break;
            case 'MERCH_REGISTER':            result = await handleMerchRegister(packet); break;
            case 'MEDIA_UPLOAD':
            case 'MEDIA_RELEASED':
            case 'BANKING_SYNC':
            case 'PROCESSOR_CONNECTED':
            case 'POLICY_UPDATED':
            case 'PENDING_SYNC':
            case 'STATS_SYNC':
            case 'REGISTRY_SYNC':
                result = await addToLedger({
                    type: packet.type, from: packet.from_wallet || 'ADMIN', to: 'SYSTEM',
                    amount: 0, token: 'RGT', extra: packet, debit: false, credit: false,
                    instruction: packet.instruction || null, packet_id: packet.packet_id || null
                }).then(() => ({ success: true }));
                break;
            default:
                result = { success: true, message: 'Pass-through', type };
        }

        await recordProcessedPacket(packet, result);
        return result;
    } catch (error) {
        console.error('❌ routePacket error:', error.message);
        return { success: false, error: error.message };
    }
}

// ============================================
// EXPRESS + SOCKET.IO
// ============================================
const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
    cors: { origin: CORS_ORIGIN, methods: ['GET', 'POST'] },
    pingTimeout: 60000, pingInterval: 25000,
    transports: ['websocket', 'polling']
});

app.use(helmet({ crossOriginResourcePolicy: { policy: 'cross-origin' }, contentSecurityPolicy: false }));
app.use(cors({ origin: CORS_ORIGIN }));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.static(__dirname));

const serverState = {
    startedAt: Date.now(), packetCount: 0, syncCount: 0,
    heartbeatCount: 0, fallbackModeActive: false,
    lastHeartbeat: null, lastHeartbeatTx: null, lastSyncTime: null,
    lastHeartbeatStatus: null
};

// ============================================
// HEALTH & STATUS
// ============================================
app.get('/health', (req, res) => {
    res.json({
        status: 'ok', systemId: SYSTEM_ID,
        role: IS_CLOUD ? 'cloud' : 'desktop',
        platform: 'node', node: process.version,
        uptime: Math.floor((Date.now() - serverState.startedAt) / 1000),
        version: '6.5', chain: 'neural', timestamp: Date.now()
    });
});
app.get('/status', async (req, res) => {
    try {
        const pendingRow = await dbGet(`SELECT COUNT(*) as c FROM sync_queue WHERE status = 'pending'`);
        res.json({
            success: true, uptime: Math.floor((Date.now() - serverState.startedAt) / 1000),
            packets: serverState.packetCount, synced: serverState.syncCount,
            pending: pendingRow ? pendingRow.c : 0,
            systemId: SYSTEM_ID, role: IS_CLOUD ? 'cloud' : 'desktop',
            heartbeatCount: serverState.heartbeatCount,
            fallbackActive: serverState.fallbackModeActive
        });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ============================================
// WHOAMI
// ============================================
app.get('/api/whoami', async (req, res) => {
    const wallet = req.headers['x-wallet'] || req.query.wallet || null;
    if (!wallet) return res.json({ success: true, wallet: null });
    try {
        const row = await dbGet('SELECT wallet, eth, name, username, role, status, client_secret, crypto_wallet, crypto_network FROM members WHERE wallet = ?', [wallet]);
        if (!row) return res.json({ success: true, wallet, known: false });
        res.json({ success: true, wallet: row.wallet, eth: row.eth, name: row.name, username: row.username, role: row.role, status: row.status, secret: row.client_secret, crypto_wallet: row.crypto_wallet, crypto_network: row.crypto_network, known: true });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ============================================
// VERIFY CLIENT
// ============================================
app.post('/api/verify-client', async (req, res) => {
    const { wallet_id, wallet, eth_address, username, voucher_number, crypto_wallet, crypto_network } = req.body;
    const targetWallet = wallet_id || wallet;

    if (!targetWallet) return res.json({ valid: false, error: 'Wallet required' });

    try {
        const member = await dbGet('SELECT * FROM members WHERE wallet = ?', [targetWallet]);
        if (!member) return res.json({ valid: false, error: 'Wallet not registered' });

        if (member.status === 'frozen') return res.json({ valid: false, error: 'Account frozen' });
        if (member.status === 'blacklisted') return res.json({ valid: false, error: 'Account blacklisted' });

        if (eth_address && member.eth && member.eth !== eth_address) {
            return res.json({ valid: false, error: 'Ethereum address mismatch' });
        }
        if (username && member.username && member.username !== username) {
            return res.json({ valid: false, error: 'Username mismatch' });
        }

        if (voucher_number) {
            const voucher = await dbGet('SELECT * FROM vouchers WHERE code = ?', [voucher_number]);
            if (!voucher) return res.json({ valid: false, error: 'Voucher not found' });
            if (voucher.used_count >= voucher.max_uses) return res.json({ valid: false, error: 'Voucher already used' });
            if (voucher.expires_at < Date.now()) return res.json({ valid: false, error: 'Voucher expired' });
            if (voucher.to_wallet && voucher.to_wallet !== targetWallet) {
                return res.json({ valid: false, error: 'Voucher not assigned to you' });
            }
        }

        if (crypto_wallet && crypto_network) {
            const rules = { trc20: /^T[a-zA-Z0-9]{33}$/, bep20: /^0x[a-fA-F0-9]{40}$/, celo: /^0x[a-fA-F0-9]{40}$/ };
            if (rules[crypto_network] && !rules[crypto_network].test(crypto_wallet)) {
                return res.json({ valid: false, error: 'Crypto wallet does not match network format' });
            }
            await dbRun('UPDATE members SET crypto_wallet = ?, crypto_network = ? WHERE wallet = ?',
                [crypto_wallet, crypto_network, targetWallet]);
        }

        res.json({
            valid: true,
            member: {
                wallet: member.wallet,
                name: member.name,
                username: member.username,
                eth: member.eth,
                role: member.role,
                status: member.status,
                expiry_date: member.expiry_date,
                crypto_wallet: crypto_wallet || member.crypto_wallet,
                crypto_network: crypto_network || member.crypto_network
            }
        });
    } catch (err) {
        res.status(500).json({ valid: false, error: err.message });
    }
});

// ============================================
// SHOW ENDPOINTS
// ============================================
app.get('/api/shows/upcoming', async (req, res) => {
    try {
        const shows = await dbAll(
            `SELECT * FROM shows WHERE status IN ('scheduled','announced') ORDER BY scheduled_at ASC LIMIT 50`
        );
        res.json({ success: true, count: shows.length, shows });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/shows/live', async (req, res) => {
    try {
        const shows = await dbAll(`SELECT * FROM shows WHERE status = 'live' ORDER BY started_at DESC`);
        res.json({ success: true, count: shows.length, shows });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/shows/:id/access', async (req, res) => {
    try {
        const showId = req.params.id;
        const wallet = req.query.wallet;
        if (!wallet) return res.json({ allowed: false, reason: 'wallet_required' });

        const show = await dbGet('SELECT * FROM shows WHERE id = ?', [showId]);
        if (!show) return res.json({ allowed: false, reason: 'show_not_found' });

        const attendance = await dbGet(
            'SELECT * FROM show_attendance WHERE show_id = ? AND attendee_wallet = ?',
            [showId, wallet]
        );

        if (!attendance) return res.json({ allowed: false, reason: 'not_purchased' });

        res.json({
            allowed: true,
            show: {
                id: show.id,
                title: show.title,
                artist_name: show.artist_name,
                status: show.status,
                video_url: show.video_url,
                started_at: show.started_at,
                duration_minutes: show.duration_minutes,
                venue: show.venue || null,
                venue_address: show.venue_address || null
            },
            attendance: {
                ticket_id: attendance.ticket_id,
                ticket_paid: attendance.ticket_paid,
                reward_received: attendance.reward_received,
                stream_earned: attendance.stream_earned,
                admission_status: attendance.admission_status,
                checked_in_at: attendance.checked_in_at
            }
        });
    } catch (err) { res.status(500).json({ allowed: false, error: err.message }); }
});

app.get('/api/shows/:id/stats', async (req, res) => {
    try {
        if (!await isAdmin(req.headers['x-wallet'] || req.query.admin_wallet)) {
            return res.status(403).json({ success: false, error: 'Admin only' });
        }
        const show = await dbGet('SELECT * FROM shows WHERE id = ?', [req.params.id]);
        if (!show) return res.status(404).json({ success: false, error: 'Not found' });
        const attendees = await dbAll('SELECT * FROM show_attendance WHERE show_id = ? ORDER BY purchased_at DESC', [req.params.id]);
        res.json({ success: true, show, attendees });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ----- v6.5: Ticket lookup for a wallet -----
app.get('/api/shows/:id/ticket/:wallet', async (req, res) => {
    try {
        const showId = req.params.id;
        const wallet = req.params.wallet;
        const show = await dbGet('SELECT * FROM shows WHERE id = ?', [showId]);
        if (!show) return res.status(404).json({ success: false, error: 'Show not found' });

        const attendance = await dbGet(
            'SELECT * FROM show_attendance WHERE show_id = ? AND attendee_wallet = ?',
            [showId, wallet]
        );
        if (!attendance) return res.status(404).json({ success: false, error: 'No pass found for this show' });

        res.json({
            success: true,
            pass: {
                ticket_id: attendance.ticket_id,
                show_id: show.id,
                show_title: show.title,
                artist_name: show.artist_name,
                scheduled_at: show.scheduled_at,
                duration_minutes: show.duration_minutes,
                venue: show.venue || null,
                venue_address: show.venue_address || null,
                check_in_opens_at: show.check_in_opens_at || null,
                ticket_paid: attendance.ticket_paid,
                ticket_token: attendance.ticket_token,
                reward_received: attendance.reward_received,
                reward_token: attendance.reward_token,
                purchased_at: attendance.purchased_at,
                checked_in: !!attendance.checked_in_at,
                checked_in_at: attendance.checked_in_at,
                admission_status: attendance.admission_status || 'pending',
                holder_wallet: attendance.attendee_wallet
            }
        });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ----- v6.5: Roll call -----
app.get('/api/shows/:id/roll-call', async (req, res) => {
    try {
        const wallet = req.headers['x-wallet'] || req.query.admin_wallet;
        if (!await isAdmin(wallet)) {
            return res.status(403).json({ success: false, error: 'Staff only' });
        }
        const showId = req.params.id;
        const show = await dbGet('SELECT * FROM shows WHERE id = ?', [showId]);
        if (!show) return res.status(404).json({ success: false, error: 'Show not found' });

        const attendees = await dbAll(
            `SELECT a.*, m.name AS attendee_name, m.username AS attendee_username
             FROM show_attendance a
             LEFT JOIN members m ON m.wallet = a.attendee_wallet
             WHERE a.show_id = ?
             ORDER BY a.checked_in_at IS NULL, a.purchased_at ASC`,
            [showId]
        );

        const total = attendees.length;
        const arrived = attendees.filter(a => a.checked_in_at).length;
        const pending = total - arrived;

        res.json({
            success: true,
            show: {
                id: show.id,
                title: show.title,
                artist_name: show.artist_name,
                scheduled_at: show.scheduled_at,
                venue: show.venue || null,
                venue_address: show.venue_address || null,
                status: show.status,
                check_in_opens_at: show.check_in_opens_at || null
            },
            counts: { total, arrived, pending },
            attendees: attendees.map(a => ({
                wallet: a.attendee_wallet,
                name: a.attendee_name || null,
                username: a.attendee_username || null,
                ticket_id: a.ticket_id,
                ticket_paid: a.ticket_paid,
                ticket_token: a.ticket_token,
                reward_received: a.reward_received,
                reward_token: a.reward_token,
                stream_earned: a.stream_earned || 0,
                stream_token: a.stream_token || null,
                purchased_at: a.purchased_at,
                checked_in_at: a.checked_in_at,
                checked_in_by: a.checked_in_by,
                admission_status: a.admission_status || 'pending',
                status: a.status
            }))
        });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ----- v6.5: Updated CSV with full column set -----
app.get('/api/shows/:id/attendees.csv', async (req, res) => {
    try {
        if (!await isAdmin(req.headers['x-wallet'] || req.query.admin_wallet)) {
            return res.status(403).send('Staff only');
        }
        const show = await dbGet('SELECT * FROM shows WHERE id = ?', [req.params.id]);
        if (!show) return res.status(404).send('Show not found');

        const attendees = await dbAll('SELECT * FROM show_attendance WHERE show_id = ? ORDER BY purchased_at ASC', [req.params.id]);

        const headers = [
            'Wallet', 'Ticket ID', 'Ticket Paid', 'Ticket Token',
            'Reward Received', 'Reward Token',
            'Stream Earned', 'Stream Token',
            'Purchased At', 'Checked In', 'Checked In At', 'Checked In By',
            'Admission Status', 'Status'
        ];
        const rows = attendees.map(a => [
            a.attendee_wallet,
            a.ticket_id || '',
            a.ticket_paid,
            a.ticket_token,
            a.reward_received,
            a.reward_token,
            a.stream_earned || 0,
            a.stream_token || '',
            a.purchased_at ? new Date(a.purchased_at).toISOString() : '',
            a.checked_in_at ? 'YES' : 'NO',
            a.checked_in_at ? new Date(a.checked_in_at).toISOString() : '',
            a.checked_in_by || '',
            a.admission_status || 'pending',
            a.status
        ]);

        const csv = [headers, ...rows].map(r => r.map(c => `"${String(c).replace(/"/g, '""')}"`).join(',')).join('\n');

        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', `attachment; filename="${show.id}_attendees.csv"`);
        res.send(csv);
    } catch (err) { res.status(500).send(err.message); }
});

app.get('/api/shows/my-attendance', async (req, res) => {
    try {
        const wallet = req.query.wallet;
        if (!wallet) return res.json({ success: false, error: 'wallet required' });
        const rows = await dbAll(
            `SELECT a.*, s.title, s.artist_name, s.scheduled_at, s.status AS show_status,
                    s.venue, s.venue_address, s.poster_url
             FROM show_attendance a 
             JOIN shows s ON s.id = a.show_id 
             WHERE a.attendee_wallet = ? 
             ORDER BY s.scheduled_at DESC LIMIT 100`,
            [wallet]
        );
        res.json({ success: true, count: rows.length, attendance: rows });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ----- v6.5: Check-in endpoint (gate staff use this) -----
app.post('/api/shows/:id/checkin', async (req, res) => {
    try {
        const showId = req.params.id;
        const { ticket_id, wallet, staff_wallet } = req.body || {};
        const staffWallet = staff_wallet || req.headers['x-wallet'];

        if (!staffWallet) return res.status(400).json({ success: false, error: 'Staff wallet required' });
        if (!await isAdmin(staffWallet)) return res.status(403).json({ success: false, error: 'Staff only' });
        if (!ticket_id && !wallet) return res.status(400).json({ success: false, error: 'ticket_id or wallet required' });

        const result = await handleShowCheckin({
            show_id: showId,
            from_wallet: staffWallet,
            ticket_id,
            attendee_wallet: wallet
        });

        if (!result.success) {
            // Friendly warm-language errors
            const friendly = {
                not_found: 'This pass could not be verified',
                already_checked_in: 'This pass was already used'
            };
            return res.status(200).json({
                success: false,
                reason: result.reason || 'unknown',
                error: friendly[result.reason] || result.error,
                checked_in_at: result.checked_in_at,
                checked_in_by: result.checked_in_by
            });
        }

        res.json({
            success: true,
            welcome: true,
            message: result.attendee_name
                ? `Welcome, ${result.attendee_name}`
                : `Welcome, ${result.attendee_wallet}`,
            attendee_wallet: result.attendee_wallet,
            attendee_name: result.attendee_name,
            ticket_id: result.ticket_id,
            checked_in_at: result.checked_in_at
        });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ============================================
// SERVER CONTROL
// ============================================
app.post('/api/heartbeat', async (req, res) => {
    try {
        const walletCount = await dbGet('SELECT COUNT(*) as c FROM members');
        const stateHash = crypto.createHash('sha256').update(String(Date.now()) + String(walletCount?.c || 0)).digest('hex');
        const fakeHash = '0x' + stateHash.slice(0, 64);

        await dbRun(
            `INSERT INTO heartbeat_log (tx_hash, state_hash, wallet_count, timestamp, status) VALUES (?, ?, ?, ?, ?)`,
            [fakeHash, stateHash, walletCount?.c || 0, Date.now(), 'simulated']
        );

        serverState.heartbeatCount++;
        serverState.lastHeartbeat = Date.now();
        serverState.lastHeartbeatTx = fakeHash;
        serverState.lastHeartbeatStatus = 'simulated';

        if (io) {
            io.emit('packet', { type: 'HEARTBEAT_SENT', tx_hash: fakeHash, wallet_count: walletCount?.c || 0, timestamp: Date.now() });
        }

        res.json({
            success: true,
            hash: fakeHash,
            state_hash: stateHash,
            wallet_count: walletCount?.c || 0,
            simulated: true,
            message: 'Heartbeat logged (BSC integration stub)'
        });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

app.get('/api/heartbeat/status', async (req, res) => {
    try {
        const last = await dbGet('SELECT * FROM heartbeat_log ORDER BY id DESC LIMIT 1');
        const alive = last && (Date.now() - last.timestamp) < 10 * 60 * 1000;
        res.json({
            success: true,
            alive: !!alive,
            lastHash: last ? last.tx_hash : null,
            lastStateHash: last ? last.state_hash : null,
            lastTimestamp: last ? last.timestamp : null,
            walletCount: last ? last.wallet_count : 0,
            totalHeartbeats: serverState.heartbeatCount,
            simulated: true
        });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

app.post('/api/fallback/activate', (req, res) => {
    serverState.fallbackModeActive = true;
    console.log('🔁 Fallback mode ACTIVATED');
    if (io) io.emit('packet', { type: 'FALLBACK_ACTIVATED', timestamp: Date.now() });
    res.json({ success: true, fallbackActive: true, message: 'Fallback mode activated' });
});

app.post('/api/fallback/deactivate', (req, res) => {
    serverState.fallbackModeActive = false;
    console.log('🔹 Primary mode RESTORED');
    if (io) io.emit('packet', { type: 'FALLBACK_DEACTIVATED', timestamp: Date.now() });
    res.json({ success: true, fallbackActive: false, message: 'Primary mode restored' });
});

app.post('/api/sync', async (req, res) => {
    try {
        const pending = await dbAll(`SELECT * FROM sync_queue WHERE status = 'pending' LIMIT 50`);
        let synced = 0;
        for (const item of pending) {
            try {
                const data = JSON.parse(item.data || '{}');
                const response = await fetch(`${FALLBACK_SERVER_URL}/api/sync/ledger`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ tx_id: item.tx_id, ...data }),
                    signal: AbortSignal.timeout(5000)
                });
                if (response.ok) {
                    await dbRun(`UPDATE sync_queue SET status = 'synced' WHERE id = ?`, [item.id]);
                    synced++;
                }
            } catch (e) {}
        }
        serverState.lastSyncTime = Date.now();
        serverState.syncCount += synced;
        res.json({ success: true, count: synced, pending: pending.length, message: `Synced ${synced} of ${pending.length}` });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

app.post('/api/start', (req, res) => res.json({ success: true, uptime: Math.floor((Date.now() - serverState.startedAt) / 1000) }));
app.post('/api/shutdown', (req, res) => { res.json({ success: true }); setTimeout(gracefulShutdown, 1500); });
app.post('/api/restart', (req, res) => { res.json({ success: true }); setTimeout(() => process.exit(0), 1000); });

// ============================================
// MEMBERS LOOKUP
// ============================================
app.get('/api/members/lookup', async (req, res) => {
    try {
        const q = req.query.q;
        if (!q) return res.status(400).json({ ok: false, error: 'q required' });
        const rows = await dbAll(
            `SELECT wallet, name, username, status, crypto_wallet, crypto_network, role
             FROM members
             WHERE wallet = ? OR username = ? OR name LIKE ?
             LIMIT 20`,
            [q, q, '%' + q + '%']
        );
        res.json({ ok: true, count: rows.length, members: rows });
    } catch (err) { res.status(500).json({ ok: false, error: err.message }); }
});

// ============================================
// CHAIN VERIFICATION
// ============================================
app.get('/api/chain/tx/:txId', async (req, res) => {
    try {
        const entry = await dbGet('SELECT * FROM ledger WHERE tx_id = ?', [req.params.txId]);
        if (!entry) return res.status(404).json({ ok: false, error: 'Not found' });
        let instruction = null;
        try { instruction = entry.instruction ? JSON.parse(entry.instruction) : null; } catch(e) {}
        res.json({
            ok: true, tx_id: entry.tx_id, type: entry.type,
            from: entry.from_wallet, to: entry.to_wallet,
            amount: entry.amount, token: entry.token, timestamp: entry.timestamp,
            instruction, instruction_valid: instruction ? true : null,
            has_signature: !!entry.packet_signature, has_packet_id: !!entry.packet_id
        });
    } catch (error) { res.status(500).json({ ok: false, error: error.message }); }
});
app.get('/api/chain/info', async (req, res) => {
    try {
        const total = await dbGet('SELECT COUNT(*) as c FROM ledger');
        const withInstruction = await dbGet('SELECT COUNT(*) as c FROM ledger WHERE instruction IS NOT NULL');
        const withSignature = await dbGet('SELECT COUNT(*) as c FROM ledger WHERE packet_signature IS NOT NULL');
        res.json({
            ok: true, chain_type: 'neural',
            total_entries: total ? total.c : 0,
            entries_with_instruction: withInstruction ? withInstruction.c : 0,
            entries_with_signature: withSignature ? withSignature.c : 0,
            verified_method: 'HMAC + idempotency + append-only ledger',
            timestamp: Date.now()
        });
    } catch (error) { res.status(500).json({ ok: false, error: error.message }); }
});

// ============================================
// PACKET
// ============================================
app.post('/api/packet', async (req, res) => {
    try {
        const wallet = req.headers['x-wallet'] || req.headers['wallet'] || 'UNKNOWN';
        const packet = req.body;
        if (!packet.from_wallet) packet.from_wallet = wallet;
        serverState.packetCount++;
        const result = await routePacket(packet);
        if (result.success && io) {
            io.emit('update', {
                type: packet.type,
                from: packet.from_wallet, to: packet.to_wallet,
                from_wallet: packet.from_wallet, to_wallet: packet.to_wallet,
                amount: packet.amount, token: packet.token,
                extra: packet, result
            });
        }
        res.json(result);
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});

// ============================================
// READ ENDPOINTS
// ============================================
app.get('/api/ledger', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 100;
        const ledger = await getLedger(limit);
        res.json({ success: true, count: ledger.length, ledger });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/members', async (req, res) => {
    try {
        const members = await dbAll('SELECT * FROM members ORDER BY registered_at DESC');
        res.json({ success: true, count: members.length, members });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/nfts', async (req, res) => {
    try {
        const nfts = await dbAll(`SELECT * FROM nfts WHERE status = 'active' ORDER BY minted_at DESC`);
        res.json({ success: true, count: nfts.length, nfts });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/feed', async (req, res) => {
    try {
        const posts = await dbAll('SELECT * FROM feed_posts ORDER BY timestamp DESC LIMIT 50');
        res.json({ success: true, count: posts.length, posts });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/works', async (req, res) => {
    try { res.json({ success: true, works: await dbAll('SELECT * FROM creative_works ORDER BY registered_at DESC') }); }
    catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/merch', async (req, res) => {
    try { res.json({ success: true, merch: await dbAll('SELECT * FROM merch_registry ORDER BY registered_at DESC') }); }
    catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/notes', async (req, res) => {
    try {
        const wallet = req.query.wallet;
        let sql = 'SELECT * FROM notes'; const params = [];
        if (wallet) { sql += ' WHERE wallet = ?'; params.push(wallet); }
        sql += ' ORDER BY updated_at DESC';
        const rows = await dbAll(sql, params);
        res.json({ success: true, count: rows.length, notes: rows });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/tokens', async (req, res) => {
    try {
        const tokenEntries = await dbAll(`SELECT * FROM ledger WHERE type LIKE 'TOKEN_FACTORY_%' OR type = 'TOKEN_MINT' ORDER BY timestamp DESC LIMIT 200`);
        const tokens = []; const seen = new Set();
        for (const entry of tokenEntries) {
            let extra = {}; try { extra = typeof entry.extra === 'string' ? JSON.parse(entry.extra) : (entry.extra || {}); } catch(e) {}
            const symbol = extra.symbol || extra.token || entry.token;
            if (symbol && !seen.has(symbol)) {
                seen.add(symbol);
                tokens.push({ symbol, name: extra.name || symbol, chain: extra.chain || 'internal', contract: extra.address || null, totalSupply: extra.supply || 0, price: extra.price || null, image: extra.image || null, slot: extra.slot || null, registeredAt: entry.timestamp });
            }
        }
        res.json({ success: true, count: tokens.length, tokens });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/messages', async (req, res) => {
    try {
        const wallet = req.query.wallet;
        let sql = 'SELECT * FROM messages'; const params = [];
        if (wallet) { sql += ' WHERE from_wallet = ? OR to_wallet = ?'; params.push(wallet, wallet); }
        sql += ' ORDER BY timestamp DESC LIMIT 300';
        const messages = await dbAll(sql, params);
        const broadcasts = await dbAll('SELECT * FROM broadcasts ORDER BY timestamp DESC LIMIT 100');
        res.json({ success: true, count: messages.length, messages, broadcasts });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/royalty', async (req, res) => {
    try {
        const ratesEntry = await dbGet(`SELECT * FROM ledger WHERE type = 'RATES_UPDATED' ORDER BY timestamp DESC LIMIT 1`);
        const streamRatesEntry = await dbGet(`SELECT * FROM ledger WHERE type = 'STREAM_RATES_UPDATED' ORDER BY timestamp DESC LIMIT 1`);
        const parseExtra = (row) => { if (!row || !row.extra) return null; try { return typeof row.extra === 'string' ? JSON.parse(row.extra) : row.extra; } catch(e) { return null; } };
        res.json({ success: true, rates: parseExtra(ratesEntry), streamRates: parseExtra(streamRatesEntry), cashBox: { RGT: await getBalance(WALLET_IDS.CASH_BOX, 'RGT') } });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/vouchers', async (req, res) => {
    try { res.json({ success: true, vouchers: await dbAll('SELECT * FROM vouchers ORDER BY created_at DESC LIMIT 500') }); }
    catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/fin107/boxes', async (req, res) => {
    try {
        const boxes = await dbAll('SELECT * FROM module_boxes ORDER BY created_at DESC');
        const parsed = boxes.map(b => ({ ...b, history: (() => { try { return JSON.parse(b.history || '[]'); } catch(e) { return []; } })() }));
        res.json({ success: true, count: parsed.length, boxes: parsed });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/wallet', async (req, res) => {
    try {
        const wallet = req.query.wallet;
        if (!wallet) return res.json({ success: false, error: 'wallet required' });
        const balances = await dbAll('SELECT token, amount FROM balances WHERE wallet = ?', [wallet]);
        const vault = balances.map(b => ({ token: b.token, balance: b.amount }));
        const analytics = {};
        const ledger = await dbAll(`SELECT type, token, amount, to_wallet FROM ledger WHERE to_wallet = ? OR from_wallet = ? ORDER BY timestamp DESC LIMIT 500`, [wallet, wallet]);
        for (const e of ledger) if (e.to_wallet === wallet) analytics[e.type] = (analytics[e.type] || 0) + (e.amount || 0);
        res.json({ success: true, wallet, vault, analytics });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/cover', async (req, res) => {
    try {
        const row = await dbGet('SELECT * FROM cover_settings ORDER BY id DESC LIMIT 1');
        let cover_bg = null;
        if (row && row.cover_bg) { try { const parsed = JSON.parse(row.cover_bg); cover_bg = parsed.cover_bg || parsed; } catch(e) { cover_bg = row.cover_bg; } }
        const pending_registrations = await dbAll(`SELECT * FROM pending_registrations WHERE status = 'pending' ORDER BY submitted_at DESC LIMIT 100`);
        res.json({ success: true, cover_bg, pending_registrations });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/storage/files', async (req, res) => {
    try {
        const uploads = await dbAll(`SELECT * FROM ledger WHERE type = 'MEDIA_UPLOAD' ORDER BY timestamp DESC LIMIT 500`);
        const releases = await dbAll(`SELECT * FROM ledger WHERE type = 'MEDIA_RELEASED' ORDER BY timestamp DESC LIMIT 500`);
        const parseExtra = (row) => { if (!row || !row.extra) return {}; try { return typeof row.extra === 'string' ? JSON.parse(row.extra) : row.extra; } catch(e) { return {}; } };
        const files = uploads.map(u => {
            const extra = parseExtra(u);
            const released = releases.some(r => parseExtra(r).mediaId === extra.mediaId);
            return { mediaId: extra.mediaId || u.tx_id, uploader: extra.uploader || u.from_wallet || 'UNKNOWN', title: extra.title || 'Untitled', mediaType: extra.mediaType || 'image', size: extra.size || 0, released, cid: extra.cid || `ipfs://ledger-${(u.tx_id || '').slice(-8)}`, timestamp: new Date(u.timestamp || Date.now()).toISOString() };
        });
        res.json({ success: true, count: files.length, files });
    } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});
app.get('/api/supply', async (req, res) => {
    try {
        const token = req.query.token;
        if (token) return res.json({ success: true, supply: await getSupply(token) });
        res.json({ success: true, supplies: await getAllSupplies() });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});
app.get('/api/rates', async (req, res) => {
    try { const rates = await getAllExchangeRates(); res.json({ ok: true, count: rates.length, rates }); }
    catch (error) { res.status(500).json({ ok: false, error: error.message }); }
});
app.get('/api/rates/:token', async (req, res) => {
    try {
        const rate = await getExchangeRate(req.params.token);
        if (rate === null) return res.status(404).json({ ok: false, error: 'Rate not found' });
        res.json({ ok: true, token: req.params.token, rate_ngn: rate });
    } catch (error) { res.status(500).json({ ok: false, error: error.message }); }
});
app.post('/api/rates/update', async (req, res) => {
    try {
        const { token, rate_ngn, currency, admin_wallet } = req.body;
        if (!await isAdmin(admin_wallet)) return res.status(403).json({ ok: false, error: 'Admin only' });
        if (!token || !rate_ngn || rate_ngn <= 0) return res.status(400).json({ ok: false, error: 'Invalid token or rate' });
        const result = await updateExchangeRate(token, rate_ngn, currency, admin_wallet);
        if (io) io.emit('packet', { type: 'EXCHANGE_RATE_UPDATED', token, rate_ngn, currency: currency || 'NGN', updated_by: admin_wallet, timestamp: Date.now() });
        await addToLedger({ type: 'EXCHANGE_RATE_UPDATED', from: admin_wallet, to: 'SYSTEM', amount: 0, token, extra: { token, rate_ngn, currency: currency || 'NGN' }, debit: false, credit: false });
        res.json(result);
    } catch (error) { res.status(500).json({ ok: false, error: error.message }); }
});
app.post('/api/cashout/settlement', async (req, res) => {
    try {
        const { wallet, token, amount, bankDetails } = req.body;
        if (!wallet || !token || !amount) return res.status(400).json({ ok: false, error: 'wallet, token, amount required' });
        const slip = await generateSettlementSlip(wallet, token, amount, bankDetails);
        if (!slip.ok) return res.status(400).json(slip);
        const result = await processSettlementSlip(slip.slip_id);
        res.json({ ...result, slip: { slip_id: slip.slip_id, token: slip.token, token_amount: slip.token_amount, rate: slip.rate, ngn_value: slip.ngn_value, crypto_wallet: slip.crypto_wallet, network: slip.network } });
    } catch (error) { res.status(500).json({ ok: false, error: error.message }); }
});
app.get('/api/cashout/slips', async (req, res) => {
    try {
        const wallet = req.query.wallet;
        let sql = 'SELECT * FROM settlement_slips'; const params = [];
        if (wallet) { sql += ' WHERE wallet = ?'; params.push(wallet); }
        sql += ' ORDER BY created_at DESC LIMIT 200';
        const slips = await dbAll(sql, params);
        res.json({ ok: true, count: slips.length, slips });
    } catch (error) { res.status(500).json({ ok: false, error: error.message }); }
});
app.get('/api/cashout/slip/:slipId', async (req, res) => {
    try {
        const slip = await dbGet('SELECT * FROM settlement_slips WHERE slip_id = ?', [req.params.slipId]);
        if (!slip) return res.status(404).json({ ok: false, error: 'Slip not found' });
        res.json({ ok: true, slip });
    } catch (error) { res.status(500).json({ ok: false, error: error.message }); }
});

// ============================================
// ADMIN REBUILD
// ============================================
app.post('/api/admin/rebuild', async (req, res) => {
    try { res.json(await rebuildStateFromLedger()); }
    catch (err) { res.status(500).json({ success: false, error: err.message }); }
});
async function rebuildStateFromLedger() {
    console.log('🔄 Rebuilding state...');
    const startTime = Date.now();
    await dbRun('DELETE FROM balances');
    await dbRun('DELETE FROM members');
    await dbRun('DELETE FROM applied_tx');
    const entries = await dbAll('SELECT * FROM ledger ORDER BY timestamp ASC, id ASC');
    let processed = 0;
    for (const entry of entries) { try { await applyLedgerEntry(entry); processed++; } catch (err) {} }
    console.log(`✅ Rebuild: ${processed}/${entries.length}`);
    return { success: true, processed, total: entries.length, elapsed: Date.now() - startTime };
}

// ============================================
// SYNC LEDGER
// ============================================
app.post('/api/sync/ledger', async (req, res) => {
    try {
        const input = req.body;
        if (!input || !input.tx_id) return res.status(400).json({ success: false, error: 'tx_id required' });
        const existing = await dbGet('SELECT tx_id FROM ledger WHERE tx_id = ?', [input.tx_id]);
        if (existing) return res.json({ success: true, message: 'Already have' });
        await dbRun(
            `INSERT INTO ledger (tx_id, type, from_wallet, to_wallet, amount, token, timestamp, status, extra, sync_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [input.tx_id, input.type, input.from_wallet, input.to_wallet, input.amount, input.token, input.timestamp || Date.now(), 'confirmed', JSON.stringify(input.extra || {}), 'synced']
        );
        await applyLedgerEntry({ tx_id: input.tx_id, type: input.type, from_wallet: input.from_wallet, to_wallet: input.to_wallet, amount: input.amount, token: input.token, timestamp: input.timestamp, extra: input.extra });
        if (io) { io.emit('ledger_entry', input); io.emit('packet', input); }
        res.json({ success: true, tx_id: input.tx_id });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ============================================
// WEBSOCKET
// ============================================
io.on('connection', (socket) => {
    console.log(`🔌 WS connected: ${socket.id}`);
    socket.on('authenticate', (wallet) => {
        if (wallet) { socket.wallet = wallet; socket.emit('authenticated', { success: true, wallet }); }
    });
    socket.on('packet', async (data) => {
        try {
            const packet = data.packet || data;
            const wallet = socket.wallet || packet.from_wallet || 'UNKNOWN';
            if (!packet.from_wallet) packet.from_wallet = wallet;
            serverState.packetCount++;
            const result = await routePacket(packet);
            socket.emit('confirmation', { original: packet, result });
            if (result.success) {
                const payload = { type: packet.type, from: packet.from_wallet, to: packet.to_wallet, from_wallet: packet.from_wallet, to_wallet: packet.to_wallet, amount: packet.amount, token: packet.token, extra: packet, result };
                socket.broadcast.emit('update', payload);
                socket.broadcast.emit('packet', payload);
            }
        } catch (err) { socket.emit('error', { error: err.message }); }
    });
    socket.on('disconnect', () => console.log(`🔌 WS disconnected: ${socket.id}`));
});

// ============================================
// MEDIA
// ============================================
const MEDIA_DIR = path.join(__dirname, 'media');
if (!fs.existsSync(MEDIA_DIR)) fs.mkdirSync(MEDIA_DIR, { recursive: true });

app.use('/media/upload', express.raw({ type: '*/*', limit: '500mb' }));
app.use('/media/mirror', express.raw({ type: '*/*', limit: '500mb' }));

app.post('/media/upload', async (req, res) => {
    try {
        if (!req.body || !req.body.length) {
            return res.status(400).json({ ok: false, error: 'Empty body' });
        }
        const hash = crypto.createHash('sha256').update(req.body).digest('hex');
        const filePath = path.join(MEDIA_DIR, hash);

        await fs.promises.writeFile(filePath, req.body);

        const meta = {
            hash,
            url: `/media/${hash}`,
            size: req.body.length,
            mime: req.query.mime || req.headers['content-type'] || 'application/octet-stream',
            title: req.query.title || '',
            creator: req.query.creator || req.headers['x-wallet'] || 'UNKNOWN',
            uploaded_at: Date.now()
        };

        if (!IS_CLOUD) {
            mirrorMediaToFallback(hash, req.body).catch(() => {});
        }

        console.log(`📼 Media uploaded: ${hash.slice(0, 12)}... (${meta.size} bytes)`);
        res.json({ ok: true, ...meta });
    } catch (err) {
        res.status(500).json({ ok: false, error: err.message });
    }
});

app.post('/media/mirror', async (req, res) => {
    try {
        const hash = req.headers['x-media-hash'];
        if (!hash || !/^[a-f0-9]{64}$/.test(hash)) {
            return res.status(400).json({ ok: false, error: 'Invalid hash' });
        }
        if (!req.body || !req.body.length) {
            return res.status(400).json({ ok: false, error: 'Empty body' });
        }
        const computed = crypto.createHash('sha256').update(req.body).digest('hex');
        if (computed !== hash) {
            return res.status(400).json({ ok: false, error: 'Hash mismatch' });
        }
        const filePath = path.join(MEDIA_DIR, hash);
        if (fs.existsSync(filePath)) {
            return res.json({ ok: true, hash, size: req.body.length, message: 'Already have' });
        }
        await fs.promises.writeFile(filePath, req.body);
        console.log(`📼 Media mirrored: ${hash.slice(0, 12)}... (${req.body.length} bytes)`);
        res.json({ ok: true, hash, size: req.body.length });
    } catch (err) {
        res.status(500).json({ ok: false, error: err.message });
    }
});

app.get('/media/:hash', (req, res) => {
    const hash = req.params.hash;
    if (!/^[a-f0-9]{64}$/.test(hash)) {
        return res.status(400).json({ ok: false, error: 'Invalid hash' });
    }
    const filePath = path.join(MEDIA_DIR, hash);
    if (!fs.existsSync(filePath)) {
        return res.status(404).json({ ok: false, error: 'Not found' });
    }
    const stat = fs.statSync(filePath);
    const range = req.headers.range;

    if (range) {
        const parts = range.replace(/bytes=/, '').split('-');
        const start = parseInt(parts[0], 10);
        const end = parts[1] ? parseInt(parts[1], 10) : stat.size - 1;
        const chunkSize = end - start + 1;
        res.writeHead(206, {
            'Content-Range': `bytes ${start}-${end}/${stat.size}`,
            'Accept-Ranges': 'bytes',
            'Content-Length': chunkSize,
            'Content-Type': 'application/octet-stream'
        });
        fs.createReadStream(filePath, { start, end }).pipe(res);
    } else {
        res.writeHead(200, {
            'Content-Length': stat.size,
            'Content-Type': 'application/octet-stream',
            'Accept-Ranges': 'bytes'
        });
        fs.createReadStream(filePath).pipe(res);
    }
});

async function mirrorMediaToFallback(hash, buffer) {
    try {
        const response = await fetch(`${FALLBACK_SERVER_URL}/media/mirror`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/octet-stream',
                'X-Media-Hash': hash
            },
            body: buffer,
            signal: AbortSignal.timeout(60000)
        });
        if (!response.ok) throw new Error('Mirror rejected: ' + response.status);
        console.log(`📼 Mirrored ${hash.slice(0, 12)}... to fallback`);
    } catch (err) {
        console.warn(`⚠️  Media mirror failed for ${hash.slice(0, 12)}...: ${err.message}`);
    }
}

// ============================================
// AUTO-SCHEDULER
// ============================================
async function runShowScheduler() {
    try {
        const now = Date.now();

        const toStart = await dbAll(
            `SELECT * FROM shows WHERE status = 'announced' AND scheduled_at <= ?`,
            [now]
        );
        for (const show of toStart) {
            try {
                await handleShowStart({ from_wallet: 'SYSTEM', show_id: show.id });
                console.log(`⏰ Auto-started show: ${show.title}`);
            } catch (e) { console.error('Auto-start error:', e.message); }
        }

        const toEnd = await dbAll(
            `SELECT * FROM shows WHERE status = 'live' AND started_at IS NOT NULL 
             AND (started_at + (duration_minutes * 60 * 1000)) <= ?`,
            [now]
        );
        for (const show of toEnd) {
            try {
                await handleShowEnd({ from_wallet: 'SYSTEM', show_id: show.id });
                console.log(`⏰ Auto-ended show: ${show.title}`);
            } catch (e) { console.error('Auto-end error:', e.message); }
        }
    } catch (err) {
        console.error('Show scheduler error:', err.message);
    }
}
setInterval(runShowScheduler, 60_000);
setTimeout(runShowScheduler, 5_000);

// ============================================
// STARTUP
// ============================================
async function startup() {
    console.log('═══════════════════════════════════════');
    console.log(`🚀 RC RECORDS SERVER v6.5 — Node ${process.version}`);
    console.log(`   Role: ${IS_CLOUD ? '☁️  CLOUD FALLBACK' : '🖥️  DESKTOP PRIMARY'}`);
    console.log(`   Chain: NEURAL`);
    console.log('═══════════════════════════════════════');
    console.log(`   System:    ${SYSTEM_ID}`);
    console.log(`   Port:      ${PORT}`);
    console.log(`   CORS:      ${CORS_ORIGIN}`);
    console.log(`   Fallback:  ${FALLBACK_SERVER_URL}`);
    console.log(`   Media dir: ${MEDIA_DIR}`);
    console.log('═══════════════════════════════════════');

    await runMigrations();

    console.log(`   ✅ All endpoints ready`);
    console.log(`   ✅ Shows + ticket system ready`);
    console.log(`   ✅ Auto-scheduler active (every 60s)`);
    console.log(`   ✅ Transfer bonuses active`);
    console.log(`   ✅ Media handlers ready`);
    console.log(`   ✅ Swap + verify-client ready`);
    console.log('═══════════════════════════════════════');

    await seedAdminMember();
}

// ============================================
// GRACEFUL SHUTDOWN
// ============================================
let shuttingDown = false;
async function gracefulShutdown() {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log('\n🛑 Shutdown');
    server.close(() => console.log('   HTTP closed'));
    await new Promise(r => setTimeout(r, 500));
    db.close((err) => { console.log('   DB closed'); process.exit(0); });
    setTimeout(() => process.exit(1), 10_000).unref();
}
process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);
process.on('uncaughtException', (err) => console.error('❌ Uncaught:', err.message));
process.on('unhandledRejection', (reason) => console.error('❌ Unhandled:', reason));

// ============================================
// START SERVER
// ============================================
server.listen(PORT, '0.0.0.0', startup);

module.exports = { app, server, io, db, routePacket };
