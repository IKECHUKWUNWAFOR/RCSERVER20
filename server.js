// ============================================
// RC RECORDS SERVER — v3.6
// Ledger + Registries + Purchase from Cash Request + Tax Withholding
// Modules decide. Server validates, executes, records.
// Neural Ledger is the single source of truth.
// ============================================

require('dotenv').config();
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const cors = require('cors');
const helmet = require('helmet');
const path = require('path');
const fs = require('fs');
const sqlite3 = require('sqlite3').verbose();
const { ethers } = require('ethers');

// ============================================
// SYSTEM IDENTIFICATION
// ============================================
const SYSTEM_ID = process.env.SYSTEM_ID || 'desktop';
console.log(`🖥️ Starting ${SYSTEM_ID} server (Node v${process.version})...`);

const PORT = process.env.PORT || 3000;
const FALLBACK_SERVER_URL = process.env.FALLBACK_SERVER_URL || 'https://records.suga.run';
const IS_CLOUD = process.env.IS_CLOUD === 'true' || SYSTEM_ID === 'suga-fallback';
const BSC_RPC_URL = process.env.BSC_RPC_URL || 'https://data-seed-prebsc-1-s1.binance.org:8545/';
const BSC_CONTRACT_ADDRESS = process.env.BSC_CONTRACT_ADDRESS || '0x4d1f190750b0c2ca61d79acd0e9669eae9e7554b';
const BSC_PRIVATE_KEY = process.env.BSC_PRIVATE_KEY || '';
const CORS_ORIGIN = process.env.CORS_ORIGIN || '*';

console.log(`   Role: ${IS_CLOUD ? '☁️  CLOUD FALLBACK' : '🖥️  DESKTOP PRIMARY'}`);

// ============================================
// SYSTEM WALLETS & CONSTANTS
// ============================================
const WALLET_IDS = {
    SYSTEM: 'RC-SYS000',
    ADMIN: 'RC-ADM456',
    CROWN_BANK: 'RC-CRN456',
    CASH_BOX: 'RC-CBX564'
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
        wallet TEXT PRIMARY KEY,
        eth TEXT,
        name TEXT,
        username TEXT,
        email TEXT,
        phone TEXT,
        address TEXT,
        role TEXT,
        tier INTEGER,
        amount_paid REAL,
        token_balance REAL DEFAULT 0,
        registered_at INTEGER,
        expiry_date TEXT,
        status TEXT DEFAULT 'active',
        client_secret TEXT
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS balances (
        wallet TEXT,
        token TEXT,
        amount REAL DEFAULT 0,
        PRIMARY KEY (wallet, token)
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tx_id TEXT UNIQUE NOT NULL,
        type TEXT NOT NULL,
        from_wallet TEXT,
        to_wallet TEXT,
        amount REAL,
        token TEXT,
        timestamp INTEGER,
        status TEXT DEFAULT 'confirmed',
        extra TEXT,
        sync_status TEXT DEFAULT 'pending_sync'
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS nfts (
        id TEXT PRIMARY KEY,
        artist_name TEXT,
        artist_wallet TEXT,
        total_shares REAL,
        shares_available REAL,
        price_per_share REAL,
        token TEXT,
        slot TEXT,
        monthly_return REAL,
        share_per_unit REAL,
        image_url TEXT,
        description TEXT,
        benefits TEXT,
        status TEXT DEFAULT 'active',
        minted_by TEXT,
        minted_at INTEGER
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS feed_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_wallet TEXT,
        message TEXT,
        image TEXT,
        category TEXT DEFAULT 'Client',
        timestamp INTEGER,
        attendCount INTEGER DEFAULT 0,
        wantCount INTEGER DEFAULT 0
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_wallet TEXT,
        to_wallet TEXT,
        body TEXT,
        image TEXT,
        timestamp INTEGER,
        read INTEGER DEFAULT 0
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS broadcasts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_wallet TEXT,
        body TEXT,
        image TEXT,
        timestamp INTEGER
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS vouchers (
        code TEXT PRIMARY KEY,
        amount REAL,
        token TEXT,
        expires_at INTEGER,
        max_uses INTEGER DEFAULT 1,
        used_count INTEGER DEFAULT 0,
        created_by TEXT,
        created_at INTEGER,
        active INTEGER DEFAULT 1,
        to_wallet TEXT
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS pending_funding (
        wallet TEXT PRIMARY KEY,
        name TEXT,
        username TEXT,
        role TEXT,
        tier INTEGER,
        amount_paid REAL,
        token_amount REAL,
        exchange_rate REAL,
        status TEXT DEFAULT 'pending',
        created_at INTEGER,
        admin TEXT
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS pending_cashouts (
        id TEXT PRIMARY KEY,
        wallet TEXT,
        amount REAL,
        currency TEXT,
        bankDetails TEXT,
        status TEXT DEFAULT 'pending',
        created_at INTEGER,
        approved_by TEXT,
        approved_at INTEGER
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS penalty_vault (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet TEXT,
        amount REAL,
        reason TEXT,
        date INTEGER
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS sync_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tx_id TEXT,
        data TEXT,
        attempts INTEGER DEFAULT 0,
        created_at INTEGER,
        status TEXT DEFAULT 'pending'
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS notes (
        id TEXT PRIMARY KEY,
        wallet TEXT,
        title TEXT,
        content TEXT,
        created_at INTEGER,
        updated_at INTEGER
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS heartbeat_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tx_hash TEXT,
        state_hash TEXT,
        wallet_count INTEGER,
        timestamp INTEGER,
        status TEXT
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS pending_registrations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        username TEXT,
        email TEXT,
        phone TEXT,
        address TEXT,
        role TEXT,
        tier INTEGER,
        voucher TEXT,
        extra_services TEXT,
        submitted_at INTEGER,
        status TEXT DEFAULT 'pending'
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS creative_works (
        id TEXT PRIMARY KEY,
        catalog_id TEXT UNIQUE,
        work_type TEXT NOT NULL,
        title TEXT NOT NULL,
        creator_wallet TEXT NOT NULL,
        creator_name TEXT,
        co_creators TEXT,
        description TEXT,
        genre TEXT,
        language TEXT,
        duration INTEGER,
        pages INTEGER,
        release_date TEXT,
        isrc TEXT,
        isbn TEXT,
        imdb_id TEXT,
        script_id TEXT,
        file_hash TEXT,
        file_url TEXT,
        cover_url TEXT,
        status TEXT DEFAULT 'registered',
        registered_by TEXT,
        registered_at INTEGER
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS merch_registry (
        id TEXT PRIMARY KEY,
        catalog_id TEXT UNIQUE,
        merch_type TEXT NOT NULL,
        title TEXT NOT NULL,
        creator_wallet TEXT NOT NULL,
        creator_name TEXT,
        description TEXT,
        category TEXT,
        linked_work_id TEXT,
        price REAL,
        token TEXT DEFAULT 'RGT',
        stock INTEGER DEFAULT 0,
        sizes TEXT,
        colors TEXT,
        materials TEXT,
        sku TEXT,
        image_url TEXT,
        image_urls TEXT,
        status TEXT DEFAULT 'registered',
        registered_by TEXT,
        registered_at INTEGER
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS purchase_quotes (
        quote_id TEXT PRIMARY KEY,
        cash_request_id TEXT,
        provider TEXT,
        network TEXT,
        asset TEXT,
        amount_ngn REAL,
        destination TEXT,
        rate REAL,
        fee_ngn REAL,
        receive_amount REAL,
        expires_at TEXT,
        status TEXT DEFAULT 'open',
        created_at INTEGER,
        wallet TEXT
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS tax_withholding (
        id TEXT PRIMARY KEY,
        cash_request_id TEXT,
        quote_id TEXT,
        wallet TEXT,
        amount_ngn REAL,
        amount_token REAL,
        asset TEXT,
        status TEXT DEFAULT 'withheld',
        created_at INTEGER,
        remitted_at INTEGER
    )`);

    console.log(`✅ Database ready: ${DB_FILE} (WAL mode)`);
});

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
// VALIDATION
// ============================================
async function validatePacket(packet) {
    if (!packet || !packet.type) return { valid: false, error: 'type required' };

    const systemTypes = ['REGISTRATION_REQUEST', 'HEARTBEAT'];
    if (!systemTypes.includes(packet.type) && !packet.from_wallet) {
        return { valid: false, error: 'from_wallet required' };
    }

    if (packet.from_wallet && typeof packet.from_wallet === 'string') {
        const valid = /^RC-\d{6}$/.test(packet.from_wallet) ||
                      /^ADMIN/.test(packet.from_wallet) ||
                      /^SYSTEM$/.test(packet.from_wallet) ||
                      /^RC-/.test(packet.from_wallet) ||
                      packet.from_wallet === 'RC-CLIENT';
        if (!valid) return { valid: false, error: 'invalid from_wallet format' };
    }

    if (packet.amount !== undefined && packet.amount !== null) {
        const amt = parseFloat(packet.amount);
        if (isNaN(amt) || amt < 0) return { valid: false, error: 'invalid amount' };
        if (amt > 1000000000) return { valid: false, error: 'amount too large' };
    }

    if (packet.token && packet.token !== 'AUTO' && packet.token !== 'NGN' && packet.token !== 'USD' && !VALID_TOKENS.includes(packet.token)) {
        return { valid: false, error: 'invalid token: ' + packet.token };
    }

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
async function isFrozen(wallet) {
    const row = await dbGet('SELECT status FROM members WHERE wallet = ?', [wallet]);
    return row && row.status === 'frozen';
}
async function isBlacklisted(wallet) {
    const row = await dbGet('SELECT status FROM members WHERE wallet = ?', [wallet]);
    return row && row.status === 'blacklisted';
}

// ============================================
// PURCHASE RATE + EXECUTION HELPERS
// ============================================
async function lookupPurchaseRate({ asset, network, amount_ngn }) {
    const base = 1520.50;
    const fee_ngn = Math.max(750, amount_ngn * 0.015);
    const receive_amount = ((amount_ngn - fee_ngn) / base);
    return {
        rate: base,
        fee_ngn: Math.round(fee_ngn),
        receive_amount: Number(receive_amount.toFixed(6))
    };
}

async function executePurchase({ quote, destination, network }) {
    try {
        // Replace this block with the real provider call when ready.
        const transaction_id = 'PROV-' + Date.now();
        return { ok: true, transaction_id };
    } catch (error) {
        return { ok: false, error: error.message };
    }
}

async function reimburseTaxToUser(quote, stampDutyNgn, stampDutyToken) {
    try {
        const reimburseResult = await executePurchase({
            quote: {
                ...quote,
                amount_ngn: stampDutyNgn,
                receive_amount: stampDutyToken
            },
            destination: quote.destination,
            network: quote.network
        });

        if (!reimburseResult.ok) {
            console.warn('⚠️ Tax reimbursement failed:', reimburseResult.error);
            return { ok: false, error: reimburseResult.error };
        }

        return { ok: true, transaction_id: reimburseResult.transaction_id };
    } catch (err) {
        return { ok: false, error: err.message };
    }
}

// ============================================
// LEDGER
// ============================================
async function addToLedger(entry) {
    const txId = 'TX_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);

    await dbRun(
        `INSERT INTO ledger (tx_id, type, from_wallet, to_wallet, amount, token, timestamp, status, extra, sync_status)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
            txId,
            entry.type,
            entry.from || entry.from_wallet || null,
            entry.to || entry.to_wallet || null,
            entry.amount || 0,
            entry.token || 'RGT',
            Date.now(),
            entry.status || 'confirmed',
            typeof entry.extra === 'string' ? entry.extra : JSON.stringify(entry.extra || {}),
            IS_CLOUD ? 'synced' : 'pending_sync'
        ]
    );

    if (entry.from && entry.amount && entry.debit !== false) {
        await updateBalance(entry.from, -entry.amount, entry.token || 'RGT');
    }
    if (entry.to && entry.amount && entry.credit !== false) {
        await updateBalance(entry.to, entry.amount, entry.token || 'RGT');
    }

    if (io) {
        io.emit('ledger_entry', {
            tx_id: txId,
            type: entry.type,
            from: entry.from || entry.from_wallet,
            to: entry.to || entry.to_wallet,
            amount: entry.amount,
            token: entry.token,
            timestamp: Date.now(),
            extra: entry.extra
        });
    }

    queueForFallbackSync(txId, entry);
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
                tx_id: txId,
                type: entry.type,
                from_wallet: entry.from || entry.from_wallet || null,
                to_wallet: entry.to || entry.to_wallet || null,
                amount: entry.amount || 0,
                token: entry.token || 'RGT',
                timestamp: Date.now(),
                extra: entry.extra || {}
            }),
            signal: AbortSignal.timeout(8000)
        });
        if (response.ok) {
            await dbRun(`UPDATE ledger SET sync_status = 'synced' WHERE tx_id = ?`, [txId]);
        } else {
            throw new Error('Fallback rejected');
        }
    } catch (err) {
        await dbRun(
            `INSERT INTO sync_queue (tx_id, data, attempts, created_at, status) VALUES (?, ?, ?, ?, ?)`,
            [txId, JSON.stringify(entry), 0, Date.now(), 'pending']
        );
    }
}

// ============================================
// LEDGER REPLAY
// ============================================
async function applyLedgerEntry(entry) {
    const extra = typeof entry.extra === 'string'
        ? (() => { try { return JSON.parse(entry.extra); } catch(e) { return {}; } })()
        : (entry.extra || {});

    switch (entry.type) {

        case 'USER_REGISTERED':
            if (extra.wallet) {
                await dbRun(
                    `INSERT OR REPLACE INTO members (wallet, eth, name, username, email, phone, address, role, tier, amount_paid, token_balance, registered_at, expiry_date, status, client_secret)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [extra.wallet, extra.eth || '', extra.name || '', extra.username || '',
                     extra.email || '', extra.phone || '', extra.address || '',
                     extra.role || 'user', extra.tier || 1, extra.amount_paid || 0,
                     extra.token_amount || 0, extra.registered_at || entry.timestamp,
                     extra.expiry_date || '', extra.status || 'active',
                     extra.client_secret || null]
                );
            }
            break;

        case 'PENDING_FUNDING_CREATED':
            if (extra.wallet) {
                await dbRun(
                    `INSERT OR REPLACE INTO pending_funding (wallet, name, username, role, tier, amount_paid, token_amount, exchange_rate, status, created_at, admin)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [extra.wallet, extra.name || '', extra.username || '', extra.role || 'user',
                     extra.tier || 1, extra.amount_paid || 0, extra.token_amount || 0,
                     extra.exchange_rate || 520, extra.status || 'pending',
                     extra.created_at || entry.timestamp, extra.admin || 'SYSTEM']
                );
            }
            break;

        case 'WALLET_DELETED':
            if (entry.to_wallet) {
                await dbRun('DELETE FROM members WHERE wallet = ?', [entry.to_wallet]);
                await dbRun('DELETE FROM balances WHERE wallet = ?', [entry.to_wallet]);
            }
            break;

        case 'TRANSFER':
        case 'P2P_TRANSFER':
        case 'PEER_SETTLEMENT':
        case 'VAULT_SEND':
        case 'VAULT_RECEIVE':
        case 'MASS_PAY':
            if (entry.from_wallet && entry.amount) await updateBalance(entry.from_wallet, -entry.amount, entry.token || 'RGT');
            if (entry.to_wallet && entry.amount) await updateBalance(entry.to_wallet, entry.amount, entry.token || 'RGT');
            if (extra.bonus_receiver) await updateBalance(entry.to_wallet, extra.bonus_receiver, entry.token || 'RGT');
            if (extra.bonus_sender) await updateBalance(entry.from_wallet, extra.bonus_sender, entry.token || 'RGT');
            if (extra.bonus_crown) await updateBalance(WALLET_IDS.CROWN_BANK, extra.bonus_crown, entry.token || 'RGT');
            break;

        case 'SWAP':
            if (entry.from_wallet && entry.amount) await updateBalance(entry.from_wallet, -entry.amount, entry.token);
            if (entry.to_wallet && entry.amount && extra.to_token) await updateBalance(entry.to_wallet, entry.amount, extra.to_token);
            break;

        case 'PURCHASE':
        case 'PURCHASE_REWARD':
            if (entry.from_wallet && entry.amount) await updateBalance(entry.from_wallet, -entry.amount, entry.token || 'RGT');
            if (entry.to_wallet && entry.amount) await updateBalance(entry.to_wallet, entry.amount, entry.token || 'RGT');
            if (extra.reward_amount && entry.from_wallet) await updateBalance(entry.from_wallet, extra.reward_amount, entry.token || 'RGT');
            break;

        case 'STREAM_REWARD':
            if (entry.to_wallet && entry.amount) await updateBalance(entry.to_wallet, entry.amount, entry.token || 'RGT');
            if (extra.listener_reward && entry.from_wallet) await updateBalance(entry.from_wallet, extra.listener_reward, entry.token || 'RGT');
            break;

        case 'NFT_MINT':
        case 'SHARE_CERTIFICATE_ISSUED':
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

        case 'NFT_SHARE_PURCHASE':
            if (extra.nft_id && extra.shares) {
                await dbRun(`UPDATE nfts SET shares_available = shares_available - ? WHERE id = ?`, [extra.shares, extra.nft_id]);
            }
            if (entry.from_wallet && entry.amount) await updateBalance(entry.from_wallet, -entry.amount, entry.token || 'RGT');
            if (entry.to_wallet && entry.amount) await updateBalance(entry.to_wallet, entry.amount, entry.token || 'RGT');
            break;

        case 'FEED_POST':
            if (extra.id) {
                await dbRun(
                    `INSERT OR REPLACE INTO feed_posts (id, from_wallet, message, image, category, timestamp, attendCount, wantCount)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
                    [extra.id, entry.from_wallet || 'UNKNOWN', extra.message || '',
                     extra.image || null, extra.category || 'Client',
                     extra.timestamp || entry.timestamp, extra.attendCount || 0, extra.wantCount || 0]
                );
            }
            break;

        case 'FEED_INTERACTION':
            if (extra.post_id && extra.interaction) {
                const col = extra.interaction === 'attend' ? 'attendCount' : 'wantCount';
                await dbRun(`UPDATE feed_posts SET ${col} = ${col} + 1 WHERE id = ?`, [extra.post_id]);
            }
            break;

        case 'P2P_MSG':
            if (entry.from_wallet && entry.to_wallet) {
                await dbRun(
                    `INSERT INTO messages (from_wallet, to_wallet, body, image, timestamp, read)
                     VALUES (?, ?, ?, ?, ?, ?)`,
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

        case 'VOUCHER_GENERATE':
            if (extra.code) {
                await dbRun(
                    `INSERT OR REPLACE INTO vouchers (code, amount, token, expires_at, max_uses, used_count, created_by, created_at, active, to_wallet)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [extra.code, extra.amount || 0, extra.token || 'REGISTRATION',
                     extra.expires_at || 0, extra.max_uses || 1, 0,
                     extra.created_by || entry.from_wallet,
                     extra.created_at || entry.timestamp, 1, extra.to_wallet || null]
                );
            }
            break;

        case 'VOUCHER_REDEEMED':
            if (extra.code) {
                await dbRun(`UPDATE vouchers SET used_count = used_count + 1 WHERE code = ?`, [extra.code]);
            }
            break;

        case 'FREEZE':
            if (entry.to_wallet) await dbRun(`UPDATE members SET status = 'frozen' WHERE wallet = ?`, [entry.to_wallet]);
            break;

        case 'UNFREEZE':
            if (entry.to_wallet) await dbRun(`UPDATE members SET status = 'active' WHERE wallet = ?`, [entry.to_wallet]);
            break;

        case 'BLACKLIST':
            if (entry.to_wallet) await dbRun(`UPDATE members SET status = 'blacklisted' WHERE wallet = ?`, [entry.to_wallet]);
            break;

        case 'UNBLACKLIST':
            if (entry.to_wallet) await dbRun(`UPDATE members SET status = 'active' WHERE wallet = ?`, [entry.to_wallet]);
            break;

        case 'NEURAL_NOTE_CREATED':
        case 'NEURAL_NOTE_UPDATED':
            if (extra.noteId) {
                await dbRun(
                    `INSERT OR REPLACE INTO notes (id, wallet, title, content, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?)`,
                    [extra.noteId, entry.from_wallet, extra.title || '', extra.content || '', entry.timestamp, Date.now()]
                );
            }
            break;

        case 'NEURAL_NOTE_DELETED':
            if (extra.noteId) await dbRun(`DELETE FROM notes WHERE id = ?`, [extra.noteId]);
            break;

        case 'NEURAL_NOTES_CLEARED':
            await dbRun(`DELETE FROM notes WHERE wallet = ?`, [entry.from_wallet]);
            break;

        case 'FARMING_BOT':
        case 'SUSPICIOUS_BOT':
        case 'SUBSCRIPTION_BOT':
        case 'MASS_COLLECT':
            if (Array.isArray(extra.users) || Array.isArray(extra.penalized) || Array.isArray(extra.collected)) {
                const list = extra.users || extra.penalized || extra.collected;
                for (const item of list) {
                    if (!item.wallet || !item.amount) continue;
                    await updateBalance(item.wallet, -item.amount, item.token || 'RGT');
                    await dbRun(
                        `INSERT INTO penalty_vault (wallet, amount, reason, date) VALUES (?, ?, ?, ?)`,
                        [item.wallet, item.amount, entry.type, entry.timestamp]
                    );
                }
            }
            break;

        case 'CASH_OUT':
        case 'CASHOUT_REQUEST':
            if (extra.requestId) {
                await dbRun(
                    `INSERT OR REPLACE INTO pending_cashouts (id, wallet, amount, currency, bankDetails, status, created_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?)`,
                    [extra.requestId, entry.from_wallet, entry.amount || 0,
                     entry.token || 'NGN', JSON.stringify(extra), 'pending', entry.timestamp]
                );
            }
            break;

        case 'CASHOUT_APPROVED':
        case 'CASHOUT_REJECTED':
            if (extra.reference) {
                const status = entry.type === 'CASHOUT_APPROVED' ? 'approved' : 'rejected';
                await dbRun(`UPDATE pending_cashouts SET status = ? WHERE id = ?`, [status, extra.reference]);
            }
            break;

        case 'REGISTRY_SYNC':
            break;

        case 'PURCHASE_QUOTE_CREATED':
            break;

        case 'PURCHASE_EXECUTED':
            if (extra.quote_id) {
                await dbRun('UPDATE purchase_quotes SET status = ? WHERE quote_id = ?',
                            ['executed', extra.quote_id]);
            }
            if (extra.cash_request_id) {
                await dbRun('UPDATE pending_cashouts SET status = ? WHERE id = ?',
                            ['fulfilled', extra.cash_request_id]);
            }
            break;

        case 'PURCHASE_FAILED':
            if (extra.quote_id) {
                await dbRun('UPDATE purchase_quotes SET status = ? WHERE quote_id = ?',
                            ['failed', extra.quote_id]);
            }
            if (extra.cash_request_id) {
                await dbRun('UPDATE pending_cashouts SET status = ? WHERE id = ?',
                            ['failed', extra.cash_request_id]);
            }
            break;

        case 'TAX_WITHHELD':
            break;

        case 'TAX_REIMBURSED':
            if (extra.cash_request_id) {
                await dbRun('UPDATE tax_withholding SET status = ? WHERE cash_request_id = ?',
                            ['reimbursed', extra.cash_request_id]);
            }
            break;

        case 'TAX_REMITTED':
            if (extra.batch_id) {
                await dbRun('UPDATE tax_withholding SET status = ?, remitted_at = ? WHERE status = ?',
                            ['remitted', Date.now(), 'reimbursed']);
            }
            break;

        case 'WORK_REGISTER':
            if (extra.work_id) {
                await dbRun(
                    `INSERT OR REPLACE INTO creative_works (
                        id, catalog_id, work_type, title, creator_wallet, creator_name,
                        co_creators, genre, status, registered_by, registered_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [
                        extra.work_id, extra.catalog_id, extra.work_type,
                        extra.title, extra.creator_wallet, extra.creator_name || '',
                        JSON.stringify(extra.co_creators || []),
                        extra.genre || '',
                        'registered', entry.from_wallet, entry.timestamp
                    ]
                );
            }
            break;

        case 'MERCH_REGISTER':
            if (extra.merch_id) {
                await dbRun(
                    `INSERT OR REPLACE INTO merch_registry (
                        id, catalog_id, merch_type, title, creator_wallet, creator_name,
                        linked_work_id, price, token, stock, status, registered_by, registered_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [
                        extra.merch_id, extra.catalog_id, extra.merch_type,
                        extra.title, extra.creator_wallet, extra.creator_name || '',
                        extra.linked_work_id || null,
                        extra.price || 0, extra.token || 'RGT', extra.stock || 0,
                        'registered', entry.from_wallet, entry.timestamp
                    ]
                );
            }
            break;

        default:
            break;
    }
}

async function rebuildStateFromLedger() {
    console.log('🔄 Rebuilding state from ledger...');
    const startTime = Date.now();

    await dbRun('DELETE FROM balances');
    await dbRun('DELETE FROM members');
    await dbRun('DELETE FROM nfts');
    await dbRun('DELETE FROM feed_posts');
    await dbRun('DELETE FROM messages');
    await dbRun('DELETE FROM broadcasts');
    await dbRun('DELETE FROM vouchers');
    await dbRun('DELETE FROM pending_funding');
    await dbRun('DELETE FROM pending_cashouts');
    await dbRun('DELETE FROM penalty_vault');
    await dbRun('DELETE FROM notes');

    const entries = await dbAll('SELECT * FROM ledger ORDER BY timestamp ASC, id ASC');
    console.log(`   Replaying ${entries.length} ledger entries...`);

    let processed = 0;
    for (const entry of entries) {
        try {
            await applyLedgerEntry(entry);
            processed++;
        } catch (err) {
            console.warn(`⚠️ Failed to replay ${entry.tx_id} (${entry.type}): ${err.message}`);
        }
    }

    const elapsed = Date.now() - startTime;
    console.log(`✅ Rebuild complete: ${processed}/${entries.length} entries replayed in ${elapsed}ms`);
    return { success: true, processed, total: entries.length, elapsed };
}

// ============================================
// HANDLERS — REGISTRATION
// ============================================
async function handleRegistrationRequest(packet) {
    const { name, username, email, phone, address, role, tier, voucher, extra_services } = packet;
    if (!name || !username || !voucher) return { success: false, error: 'Name, username, and voucher required' };

    await dbRun(
        `INSERT INTO pending_registrations (name, username, email, phone, address, role, tier, voucher, extra_services, submitted_at, status)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [name, username, email || '', phone || '', address || '', role || 'user', tier || null, voucher,
         JSON.stringify(extra_services || []), Date.now(), 'pending']);

    await addToLedger({
        type: 'REGISTRATION_REQUEST',
        from: 'REGISTRATION_FORM',
        to: 'ONBOARDING_PENDING',
        amount: 0,
        token: 'RGT',
        extra: { name, username, role, tier, voucher },
        debit: false,
        credit: false
    });

    return { success: true, message: 'Registration submitted — awaiting admin approval' };
}

async function handleUserRegistration(packet) {
    const data = packet.data || packet;
    const { name, username, email, phone, address, role, tier, amount_paid, adminWallet } = data;

    if (!await isAdmin(packet.from_wallet)) {
        return { success: false, error: 'Unauthorized — admin only' };
    }

    const wallet = packet.wallet || data.wallet || generateWalletId();
    const eth = packet.eth || data.eth || generateEthAddress();
    const tokenAmount = packet.token_amount || data.token_amount || (amount_paid / 520);
    const expiryDate = packet.expiry_date || data.expiry_date || calculateExpiry(tier);
    const clientSecret = packet.client_secret || data.client_secret || null;

    if (!name || !username || !amount_paid) return { success: false, error: 'Name, username, and amount_paid required' };

    const existing = await dbGet('SELECT wallet FROM members WHERE wallet = ?', [wallet]);
    if (existing) return { success: false, error: 'Wallet already registered' };

    const existingUsername = await dbGet('SELECT username FROM members WHERE username = ?', [username]);
    if (existingUsername) return { success: false, error: 'Username already exists' };

    await dbRun(
        `INSERT INTO members (wallet, eth, name, username, email, phone, address, role, tier, amount_paid, token_balance, registered_at, expiry_date, status, client_secret)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [wallet, eth, name, username, email || '', phone || '', address || '', role || 'user', tier, amount_paid, 0, Date.now(), expiryDate, 'active', clientSecret]
    );

    await dbRun(
        `INSERT INTO pending_funding (wallet, name, username, role, tier, amount_paid, token_amount, exchange_rate, status, created_at, admin)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [wallet, name, username, role, tier, amount_paid, tokenAmount, 520, 'pending', Date.now(), adminWallet || 'SYSTEM']
    );

    await addToLedger({
        type: 'USER_REGISTERED',
        from: 'SYSTEM',
        to: wallet,
        amount: amount_paid,
        token: 'NGN',
        extra: {
            wallet: wallet, eth: eth, name: name, username: username,
            email: email || '', phone: phone || '', address: address || '',
            role: role || 'user', tier: tier, amount_paid: amount_paid,
            token_amount: tokenAmount, exchange_rate: 520,
            expiry_date: expiryDate, registered_at: Date.now(),
            status: 'active', admin: adminWallet || 'SYSTEM',
            client_secret: clientSecret
        },
        debit: false,
        credit: false
    });

    await addToLedger({
        type: 'PENDING_FUNDING_CREATED',
        from: 'SYSTEM',
        to: wallet,
        amount: tokenAmount,
        token: 'RGT',
        extra: {
            wallet: wallet, name: name, username: username, role: role,
            tier: tier, amount_paid: amount_paid, token_amount: tokenAmount,
            exchange_rate: 520, status: 'pending',
            created_at: Date.now(), admin: adminWallet || 'SYSTEM'
        },
        debit: false,
        credit: false
    });

    return { success: true, walletId: wallet, ethAddress: eth, tokenAmount, clientSecret };
}

async function handleWalletDeleted(packet) {
    const { wallet, name } = packet;

    if (!await isAdmin(packet.from_wallet)) {
        return { success: false, error: 'Unauthorized — admin only' };
    }

    if (!wallet) return { success: false, error: 'Wallet required' };

    await dbRun('DELETE FROM members WHERE wallet = ?', [wallet]);
    await dbRun('DELETE FROM balances WHERE wallet = ?', [wallet]);

    await addToLedger({
        type: 'WALLET_DELETED',
        from: 'ADMIN',
        to: wallet,
        amount: 0,
        token: 'RGT',
        extra: { name: name || '', wallet: wallet },
        debit: false,
        credit: false
    });

    return { success: true };
}

async function handleRatesUpdated(packet) {
    if (!await isAdmin(packet.from_wallet)) {
        return { success: false, error: 'Unauthorized — admin only' };
    }

    await addToLedger({
        type: 'RATES_UPDATED',
        from: packet.from_wallet || 'ADMIN',
        to: 'SYSTEM',
        amount: 0,
        token: 'RGT',
        extra: {
            exchange: packet.exchange_rate,
            extract: packet.extract_percent,
            farming: packet.farming_percent,
            penalty: packet.universal_penalty
        },
        debit: false,
        credit: false
    });
    return { success: true };
}

// ============================================
// HANDLERS — TRANSACTIONS
// ============================================
async function handleTransfer(packet) {
    const { from_wallet, to_wallet, amount, token } = packet;
    if (!await walletExists(from_wallet)) return { success: false, error: 'Sender not found' };
    if (!await walletExists(to_wallet)) return { success: false, error: 'Recipient not found' };
    if (from_wallet === to_wallet) return { success: false, error: 'Self-transfer not allowed' };
    if (await isFrozen(from_wallet)) return { success: false, error: 'Wallet frozen' };
    if (await isBlacklisted(from_wallet)) return { success: false, error: 'Wallet blacklisted' };
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
        type: 'TRANSFER',
        from: from_wallet,
        to: to_wallet,
        amount: amount,
        token: token,
        extra: {
            bonus_receiver: bonusRecipient,
            bonus_sender: bonusSender,
            bonus_crown: bonusCrown
        },
        debit: false,
        credit: false
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

    for (const recipient of recipients) {
        if (!await walletExists(recipient.wallet)) continue;
        await updateBalance(recipient.wallet, recipient.amount, token);
        await dbRun(`UPDATE pending_funding SET status = 'funded' WHERE wallet = ?`, [recipient.wallet]);
        await addToLedger({
            type: 'MASS_PAY',
            from: WALLET_IDS.ADMIN,
            to: recipient.wallet,
            amount: recipient.amount,
            token: token,
            extra: { name: recipient.name || '' },
            debit: false,
            credit: false
        });
    }

    return { success: true, total: totalAmount, count: recipients.length };
}

async function handleMassPayBatch(packet) {
    const { from_wallet, total_amount, recipient_count, token } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };

    await addToLedger({
        type: 'MASS_PAY_BATCH',
        from: from_wallet,
        to: 'MULTIPLE',
        amount: total_amount || 0,
        token: token || 'RGT',
        extra: { recipients: recipient_count || 0 },
        debit: false,
        credit: false
    });
    return { success: true };
}

async function handleSwap(packet) {
    const { from_wallet, to_wallet, from_token, to_token, amount } = packet;
    if (!await walletExists(from_wallet)) return { success: false, error: 'Wallet not found' };
    if (await isFrozen(from_wallet)) return { success: false, error: 'Wallet frozen' };
    if (await isBlacklisted(from_wallet)) return { success: false, error: 'Wallet blacklisted' };
    if (!isValidToken(from_token)) return { success: false, error: 'Invalid from_token' };

    const balance = await getBalance(from_wallet, from_token);
    if (balance < amount) return { success: false, error: 'Insufficient balance' };

    await updateBalance(from_wallet, -amount, from_token);
    if (to_token && isValidToken(to_token)) {
        await updateBalance(to_wallet || from_wallet, amount, to_token);
    }

    await addToLedger({
        type: 'SWAP',
        from: from_wallet,
        to: to_wallet || from_wallet,
        amount: amount,
        token: from_token,
        extra: { to_token },
        debit: false,
        credit: false
    });

    return { success: true };
}

async function handlePeerTransfer(packet) {
    const { from_wallet, to_wallet, amount, token } = packet;
    if (!await walletExists(from_wallet)) return { success: false, error: 'Sender not found' };
    if (!await walletExists(to_wallet)) return { success: false, error: 'Recipient not found' };
    if (amount <= 0) return { success: false, error: 'Invalid amount' };

    const balance = await getBalance(from_wallet, token || 'RGT');
    if (balance < amount) return { success: false, error: 'Insufficient balance' };

    await updateBalance(from_wallet, -amount, token || 'RGT');
    await updateBalance(to_wallet, amount, token || 'RGT');

    await addToLedger({
        type: 'P2P_TRANSFER',
        from: from_wallet,
        to: to_wallet,
        amount: amount,
        token: token || 'RGT',
        debit: false,
        credit: false
    });

    return { success: true };
}

async function handleVaultSend(packet) { return await handleTransfer(packet); }
async function handleVaultReceive(packet) { return await handleTransfer(packet); }

async function deductWithFallback(wallet, amount, preferredToken) {
    const tokenOrder = [preferredToken, 'RGT', 'RCT', 'IRT', 'RT', 'ET']
        .filter((v, i, a) => a.indexOf(v) === i);

    for (const token of tokenOrder) {
        const balance = await getBalance(wallet, token);
        if (balance >= amount) {
            await updateBalance(wallet, -amount, token);
            return { success: true, token: token, deducted: amount };
        }
    }
    return { success: false, error: 'Insufficient in all tokens' };
}

async function handlePurchase(packet) {
    const { from_wallet, to_wallet, product_id, product_type, registry_location, registry_button } = packet;

    let amount = packet.amount;
    let reward_amount = packet.reward_amount || 0;
    let token = packet.token;
    let sellerWallet = to_wallet;

    if (!amount || amount === 0 || !token || token === 'AUTO') {
        const lastSync = await dbGet(
            `SELECT * FROM ledger WHERE type = 'REGISTRY_SYNC' ORDER BY timestamp DESC LIMIT 1`
        );

        if (!lastSync) {
            return { success: false, error: 'No registry in ledger', silent: true };
        }

        const extra = typeof lastSync.extra === 'string' ? JSON.parse(lastSync.extra) : lastSync.extra;
        const registry = extra.registry || [];

        const registryRow = registry.find(r =>
            r.location === registry_location &&
            r.buttonType === registry_button &&
            r.targetWallet === to_wallet
        );

        if (!registryRow) {
            return { success: false, error: 'No registry match', silent: true };
        }

        amount = registryRow.deductionAmount;
        reward_amount = registryRow.rewardAmount;
        token = registryRow.token;
        sellerWallet = registryRow.targetWallet;
    }

    if (!await walletExists(from_wallet)) return { success: false, error: 'Buyer not found', silent: true };
    if (await isFrozen(from_wallet)) return { success: false, error: 'Wallet frozen', silent: true };
    if (amount <= 0) return { success: false, error: 'Invalid amount', silent: true };

    if (reward_amount > 0) {
        const vaultBalance = await getBalance(WALLET_IDS.ADMIN, token);
        if (vaultBalance >= reward_amount) {
            await updateBalance(from_wallet, reward_amount, token);
            await updateBalance(WALLET_IDS.ADMIN, -reward_amount, token);

            if (io) {
                io.emit('ledger_entry', {
                    type: 'PURCHASE_REWARD',
                    from: 'VAULT',
                    to: from_wallet,
                    amount: reward_amount,
                    token: token,
                    extra: { product_id, message: `🎁 Reward: +${reward_amount} ${token}` },
                    timestamp: Date.now()
                });
            }
        }
    }

    const deduction = await deductWithFallback(from_wallet, amount, token);
    if (!deduction.success) {
        return { success: false, error: 'Insufficient balance', silent: true };
    }

    if (sellerWallet && await walletExists(sellerWallet)) {
        await updateBalance(sellerWallet, amount, deduction.token);
    }

    await addToLedger({
        type: 'PURCHASE',
        from: from_wallet,
        to: sellerWallet,
        amount: amount,
        token: deduction.token,
        extra: {
            product_id, product_type, reward_amount,
            reward_first: true, net_change: reward_amount - amount,
            token_used: deduction.token, preferred_token: token,
            silent: true
        },
        debit: false,
        credit: false
    });

    return { success: true, amount, reward_amount, token: deduction.token };
}

async function handleProductInterest(packet) {
    const { from_wallet, to_wallet, amount, token, product_id, product_type } = packet;
    await addToLedger({
        type: 'PRODUCT_INTEREST',
        from: from_wallet,
        to: to_wallet || 'SYSTEM',
        amount: amount || 0,
        token: token || 'RGT',
        extra: { product_id, product_type },
        debit: false,
        credit: false
    });
    return { success: true };
}

async function handleEventAttend(packet) {
    const { from_wallet, to_wallet, amount, token, eventId, eventTitle } = packet;
    await addToLedger({
        type: 'EVENT_ATTEND',
        from: from_wallet,
        to: to_wallet || 'EVENT_SYSTEM',
        amount: amount || 0,
        token: token || 'RGT',
        extra: { eventId, eventTitle },
        debit: false,
        credit: false
    });
    return { success: true };
}

async function handleStreamReward(packet) {
    const { from_wallet, to_wallet, amount, token, listener_reward, media_title, play_percentage } = packet;
    if (play_percentage && play_percentage < 30) return { success: false, error: 'Stream below 30%' };

    const cashBoxBalance = await getBalance(WALLET_IDS.CASH_BOX, token);
    const totalRequired = amount + (listener_reward || 0);
    if (cashBoxBalance < totalRequired) return { success: false, error: 'Insufficient CASH_BOX' };

    await updateBalance(WALLET_IDS.CASH_BOX, -totalRequired, token);
    if (amount > 0) await updateBalance(to_wallet, amount, token);
    if (listener_reward > 0 && from_wallet) await updateBalance(from_wallet, listener_reward, token);

    await addToLedger({
        type: 'STREAM_REWARD',
        from: from_wallet,
        to: to_wallet,
        amount: amount,
        token: token,
        extra: { media_title, listener_reward },
        debit: false,
        credit: false
    });
    return { success: true };
}

async function handleStreamRatesUpdated(packet) {
    if (!await isAdmin(packet.from_wallet)) {
        return { success: false, error: 'Unauthorized — admin only' };
    }

    await addToLedger({
        type: 'STREAM_RATES_UPDATED',
        from: packet.from_wallet || 'ADMIN',
        to: 'SYSTEM',
        amount: 0,
        token: 'RGT',
        extra: packet.rates,
        debit: false,
        credit: false
    });
    return { success: true };
}

async function handleMediaUpload(packet) {
    await addToLedger({
        type: 'MEDIA_UPLOAD',
        from: packet.uploader || packet.from_wallet,
        to: 'NEURAL_NET',
        amount: packet.size || 0,
        token: 'RGT',
        extra: { title: packet.title, mediaId: packet.mediaId },
        debit: false,
        credit: false
    });
    return { success: true };
}

async function handleMediaReleased(packet) {
    await addToLedger({
        type: 'MEDIA_RELEASED',
        from: packet.uploader || packet.from_wallet,
        to: 'NEURAL_NET',
        amount: 0,
        token: 'RGT',
        extra: { title: packet.title, mediaId: packet.mediaId },
        debit: false,
        credit: false
    });
    return { success: true };
}

async function handleBroadcast(packet) {
    const { from_wallet, message, image, body } = packet;
    const msgBody = body || message || '';

    await dbRun(`INSERT INTO broadcasts (from_wallet, body, image, timestamp) VALUES (?, ?, ?, ?)`,
        [from_wallet, msgBody, image || null, Date.now()]);

    await addToLedger({
        type: 'BROADCAST_MSG',
        from: from_wallet,
        to: 'ALL',
        amount: 0,
        token: 'RGT',
        extra: { message: msgBody, image: image || null },
        debit: false,
        credit: false
    });
    return { success: true };
}

async function handleBotReach(packet) {
    await addToLedger({
        type: 'BOT_REACH',
        from: packet.from_wallet || 'ADMIN',
        to: 'ALL',
        amount: 0,
        token: 'RGT',
        extra: { action: packet.action || 'reach_all_players' },
        debit: false,
        credit: false
    });
    return { success: true };
}

// ============================================
// HANDLERS — NFT
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
    const description = data.description || '';
    const benefits = data.benefits || '';

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
         description, benefits, 'active', from_wallet, Date.now()]
    );

    await addToLedger({
        type: 'NFT_MINT',
        from: from_wallet,
        to: 'ALL',
        amount: total_shares * price_per_share,
        token: token,
        extra: {
            id: nftId, nft_id: nftId,
            artist_name: artist_name, artist_wallet: artist_wallet,
            title: artist_name + ' - Share Certificate',
            total_shares: total_shares, price_per_share: price_per_share,
            monthly_return: monthly_return, share_per_unit: share_per_unit,
            token: token, slot: slot || '',
            image_url: image_url || '', image: image_url || '',
            description: description, benefits: benefits,
            total_value: total_shares * price_per_share
        },
        debit: false,
        credit: false
    });

    console.log(`✅ NFT minted & broadcast to ALL: ${nftId}`);
    return { success: true, nft_id: nftId };
}

async function handleNFTSharePurchase(packet) {
    const { from_wallet, nft_id, shares } = packet;
    if (!await walletExists(from_wallet)) return { success: false, error: 'Buyer not found' };
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
        type: 'NFT_SHARE_PURCHASE',
        from: from_wallet,
        to: nft.artist_wallet,
        amount: totalCost,
        token: nft.token,
        extra: { nft_id, shares, artist_name: nft.artist_name },
        debit: false,
        credit: false
    });
    return { success: true, shares, totalCost };
}

async function handleNFTPurchase(packet) { return await handleNFTSharePurchase(packet); }
async function handleShareCertificateIssued(packet) {
    const payload = packet.payload || packet.data || packet;
    return await handleNFTMint({ ...packet, ...payload, data: payload });
}
async function handleSharePurchase(packet) { return await handleNFTSharePurchase(packet); }

// ============================================
// HANDLERS — FEED & MESSAGES
// ============================================
async function handleFeedPost(packet) {
    const { from_wallet, message, image, category, post_id } = packet;
    if (!message && !image) return { success: false, error: 'Message or image required' };

    const postId = post_id || Date.now();

    await dbRun(
        `INSERT INTO feed_posts (id, from_wallet, message, image, category, timestamp, attendCount, wantCount)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [postId, from_wallet || 'UNKNOWN', message || '', image || null, category || 'Client', Date.now(), 0, 0]
    );

    await addToLedger({
        type: 'FEED_POST',
        from: from_wallet || 'UNKNOWN',
        to: 'ALL',
        amount: 0,
        token: 'RGT',
        extra: {
            id: postId, message: message || '', image: image || '',
            category: category || 'Client', wallet: from_wallet,
            timestamp: Date.now(), attendCount: 0, wantCount: 0
        },
        debit: false,
        credit: false
    });
    return { success: true };
}

async function handleFeedPostCreated(packet) {
    await addToLedger({
        type: 'FEED_POST_CREATED',
        from: packet.from_wallet || 'UNKNOWN',
        to: 'SYSTEM',
        amount: 0,
        token: 'RGT',
        extra: { post_id: packet.post_id },
        debit: false,
        credit: false
    });
    return { success: true };
}

async function handleFeedInteraction(packet) {
    const { from_wallet, post_id, interaction } = packet;
    if (!post_id || !interaction) return { success: false, error: 'post_id and interaction required' };

    const column = interaction === 'attend' ? 'attendCount' : 'wantCount';
    await dbRun(`UPDATE feed_posts SET ${column} = ${column} + 1 WHERE id = ?`, [post_id]);

    const post = await dbGet(`SELECT * FROM feed_posts WHERE id = ?`, [post_id]);
    if (!post) return { success: false, error: 'Post not found' };

    await addToLedger({
        type: 'FEED_INTERACTION',
        from: from_wallet,
        to: 'ALL',
        amount: 0,
        token: 'RGT',
        extra: {
            post_id: post_id, interaction: interaction,
            attendCount: post.attendCount, wantCount: post.wantCount
        },
        debit: false,
        credit: false
    });
    return { success: true, attendCount: post.attendCount, wantCount: post.wantCount };
}

async function handleP2PMessage(packet) {
    const { from_wallet, to_wallet, body, image } = packet;
    if (!body || body.trim() === '') return { success: false, error: 'Message required' };

    await dbRun(
        `INSERT INTO messages (from_wallet, to_wallet, body, image, timestamp, read)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [from_wallet, to_wallet, body, image || null, Date.now(), 0]
    );

    await addToLedger({
        type: 'P2P_MSG',
        from: from_wallet,
        to: to_wallet,
        amount: 0,
        token: 'RGT',
        extra: { message: body, image: image || null },
        debit: false,
        credit: false
    });
    return { success: true };
}

async function handleMassMessage(packet) {
    await addToLedger({
        type: 'MASS_MSG',
        from: packet.from_wallet,
        to: packet.to_wallet || 'MULTIPLE',
        amount: 0,
        token: 'RGT',
        extra: { message: (packet.body || '').substring(0, 100) },
        debit: false,
        credit: false
    });
    return { success: true };
}

// ============================================
// HANDLERS — CASHOUT
// ============================================
async function handleCashOut(packet) {
    const { from_wallet, to_wallet, amount, currency, name, email, bank, account, processor, reference } = packet;
    if (!await walletExists(from_wallet)) return { success: false, error: 'Wallet not found' };
    if (amount <= 0) return { success: false, error: 'Invalid amount' };

    const requestId = reference || 'CASHOUT_' + Date.now();

    await dbRun(
        `INSERT INTO pending_cashouts (id, wallet, amount, currency, bankDetails, status, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [requestId, from_wallet, amount, currency || 'NGN',
         JSON.stringify({ name, email, bank, account, processor }), 'pending', Date.now()]
    );

    await addToLedger({
        type: 'CASH_OUT',
        from: from_wallet,
        to: to_wallet || 'BANKING_SYSTEM',
        amount: amount,
        token: currency || 'NGN',
        extra: {
            requestId, processor, name, email, bank, account,
            currency: currency || 'NGN'
        },
        debit: false,
        credit: false
    });
    return { success: true, requestId };
}

async function handleCashoutRequest(packet) { return await handleCashOut(packet); }

async function handleCashoutApproved(packet) {
    const { from_wallet, to_wallet, amount, currency, reference, request_id } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };

    const id = request_id || reference;
    if (id) await dbRun(`UPDATE pending_cashouts SET status = 'approved', approved_by = ?, approved_at = ? WHERE id = ?`, [from_wallet, Date.now(), id]);

    await addToLedger({
        type: 'CASHOUT_APPROVED',
        from: 'ADMIN', to: to_wallet,
        amount: amount || 0, token: currency || 'NGN',
        extra: { reference: id },
        debit: false, credit: false
    });
    return { success: true };
}

async function handleCashoutRejected(packet) {
    const { from_wallet, to_wallet, amount, reason, reference, request_id } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };

    const id = request_id || reference;
    if (id) await dbRun(`UPDATE pending_cashouts SET status = 'rejected', approved_by = ?, approved_at = ? WHERE id = ?`, [from_wallet, Date.now(), id]);

    await addToLedger({
        type: 'CASHOUT_REJECTED',
        from: 'ADMIN', to: to_wallet,
        amount: amount || 0, token: 'NGN',
        extra: { reference: id, reason: reason || 'Admin rejected' },
        debit: false, credit: false
    });
    return { success: true };
}

// ============================================
// HANDLERS — BANKING
// ============================================
async function handleBankingSync(packet) {
    await addToLedger({ type: 'BANKING_SYNC', from: packet.from_wallet || 'ADMIN', to: 'SYSTEM', amount: 0, token: 'RGT', extra: packet.data, debit: false, credit: false });
    return { success: true };
}

async function handleProcessorConnected(packet) {
    if (!await isAdmin(packet.from_wallet)) {
        return { success: false, error: 'Unauthorized — admin only' };
    }
    await addToLedger({ type: 'PROCESSOR_CONNECTED', from: packet.from_wallet || 'ADMIN', to: 'SYSTEM', amount: 0, token: 'RGT', extra: { name: packet.processor_name, slot: packet.slot }, debit: false, credit: false });
    return { success: true };
}

async function handlePolicyUpdated(packet) {
    if (!await isAdmin(packet.from_wallet)) {
        return { success: false, error: 'Unauthorized — admin only' };
    }
    await addToLedger({ type: 'POLICY_UPDATED', from: packet.from_wallet || 'ADMIN', to: 'SYSTEM', amount: 0, token: 'RGT', extra: packet.policy, debit: false, credit: false });
    return { success: true };
}

// ============================================
// HANDLERS — PURCHASE (CASH REQUEST → STABLECOIN)
// ============================================
async function handlePurchaseQuote(packet) {
    const { cash_request_id, asset, destination, network, provider } = packet;

    if (!cash_request_id) {
        return { ok: false, message: 'Cash request ID required.' };
    }

    const cashRequest = await dbGet('SELECT * FROM pending_cashouts WHERE id = ?', [cash_request_id]);
    if (!cashRequest) {
        return { ok: false, message: 'Cash request not found.' };
    }
    if (cashRequest.status === 'fulfilled') {
        return { ok: false, message: 'Cash request already fulfilled.' };
    }

    const allowed = { celo: ['USDC'], bep20: ['USDT'], trc20: ['USDT'] };
    if (!allowed[network] || !allowed[network].includes(asset)) {
        return { ok: false, message: 'Asset not available on that network.' };
    }

    const rules = {
        celo:  /^0x[a-fA-F0-9]{40}$/,
        bep20: /^0x[a-fA-F0-9]{40}$/,
        trc20: /^T[a-zA-Z0-9]{33}$/
    };
    if (!rules[network] || !rules[network].test(destination)) {
        return { ok: false, message: 'Destination address invalid for network.' };
    }

    const amount = parseFloat(cashRequest.amount);
    if (isNaN(amount) || amount <= 0) {
        return { ok: false, message: 'Invalid cash amount.' };
    }

    const quote = await lookupPurchaseRate({ asset, network, amount_ngn: amount });
    const quote_id = 'RC-Q-' + Date.now() + '_' + Math.random().toString(36).substr(2, 4);
    const expires_at = new Date(Date.now() + 15 * 60 * 1000).toISOString();

    await dbRun(
        `INSERT INTO purchase_quotes
         (quote_id, cash_request_id, provider, network, asset, amount_ngn, destination,
          rate, fee_ngn, receive_amount, expires_at, status, created_at, wallet)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
        [quote_id, cash_request_id, provider, network, asset, amount, destination,
         quote.rate, quote.fee_ngn, quote.receive_amount,
         expires_at, 'open', Date.now(), cashRequest.wallet]
    );

    await addToLedger({
        type: 'PURCHASE_QUOTE_CREATED',
        from: 'BANKING_API',
        to: 'PURCHASE_QUOTE',
        amount: amount,
        token: asset,
        extra: { quote_id, cash_request_id, provider, network, destination },
        debit: false,
        credit: false
    });

    return {
        ok: true, quote_id, cash_request_id, asset,
        amount_ngn: amount,
        rate: quote.rate, fee_ngn: quote.fee_ngn,
        receive_amount: quote.receive_amount,
        expires_at
    };
}

async function handlePurchaseExecute(packet) {
    const { quote_id } = packet;
    const quote = await dbGet('SELECT * FROM purchase_quotes WHERE quote_id = ?', [quote_id]);
    if (!quote) return { ok: false, message: 'Quote not found.' };
    if (quote.status !== 'open') return { ok: false, message: 'Quote already used.' };
    if (Date.now() > new Date(quote.expires_at).getTime()) {
        await dbRun('UPDATE purchase_quotes SET status = ? WHERE quote_id = ?', ['expired', quote_id]);
        return { ok: false, message: 'Quote expired.' };
    }

    const result = await executePurchase({
        quote,
        destination: quote.destination,
        network: quote.network
    });

    if (!result.ok) {
        await dbRun('UPDATE purchase_quotes SET status = ? WHERE quote_id = ?', ['failed', quote_id]);
        await dbRun('UPDATE pending_cashouts SET status = ? WHERE id = ?', ['failed', quote.cash_request_id]);
        await addToLedger({
            type: 'PURCHASE_FAILED',
            from: 'BANKING_API',
            to: 'PURCHASE_QUOTE',
            amount: quote.amount_ngn,
            token: quote.asset,
            extra: { quote_id, cash_request_id: quote.cash_request_id, error: result.error },
            debit: false, credit: false
        });
        return { ok: false, message: result.error };
    }

    await dbRun('UPDATE purchase_quotes SET status = ? WHERE quote_id = ?', ['executed', quote_id]);
    await dbRun('UPDATE pending_cashouts SET status = ? WHERE id = ?', ['fulfilled', quote.cash_request_id]);

    const reference = 'RC-PUR-' + Date.now();
    await addToLedger({
        type: 'PURCHASE_EXECUTED',
        from: 'BANKING_API',
        to: 'USER_WALLET',
        amount: quote.amount_ngn,
        token: quote.asset,
        extra: {
            reference, quote_id,
            cash_request_id: quote.cash_request_id,
            provider: quote.provider,
            network: quote.network,
            destination: quote.destination,
            transaction_id: result.transaction_id
        },
        debit: false, credit: false
    });

    const stampDutyNgn = Math.round(quote.amount_ngn * 0.015);
    const stampDutyToken = Number((stampDutyNgn / quote.rate).toFixed(6));

    await dbRun(
        `INSERT INTO tax_withholding
         (id, cash_request_id, quote_id, wallet, amount_ngn, amount_token, asset,
          status, created_at)
         VALUES (?,?,?,?,?,?,?,?,?)`,
        [
            'TAX_' + Date.now(),
            quote.cash_request_id,
            quote_id,
            quote.wallet,
            stampDutyNgn,
            stampDutyToken,
            quote.asset,
            'withheld',
            Date.now()
        ]
    );

    await addToLedger({
        type: 'TAX_WITHHELD',
        from: 'USER_WALLET',
        to: 'TAX_VAULT',
        amount: stampDutyNgn,
        token: quote.asset,
        extra: {
            quote_id,
            cash_request_id: quote.cash_request_id,
            stamp_duty_ngn: stampDutyNgn,
            stamp_duty_token: stampDutyToken
        },
        debit: false, credit: false
    });

    const reimburse = await reimburseTaxToUser(quote, stampDutyNgn, stampDutyToken);

    if (reimburse.ok) {
        await dbRun('UPDATE tax_withholding SET status = ? WHERE cash_request_id = ?',
                    ['reimbursed', quote.cash_request_id]);

        await addToLedger({
            type: 'TAX_REIMBURSED',
            from: 'PLATFORM_OPERATING',
            to: 'USER_WALLET',
            amount: stampDutyNgn,
            token: quote.asset,
            extra: {
                quote_id,
                cash_request_id: quote.cash_request_id,
                reimbursement_tx: reimburse.transaction_id,
                note: 'Platform absorbs stamp duty on behalf of user'
            },
            debit: false, credit: false
        });
    }

    return {
        ok: true, reference,
        cash_request_id: quote.cash_request_id,
        provider: quote.provider,
        network: quote.network,
        asset: quote.asset,
        amount_ngn: quote.amount_ngn,
        received_amount: quote.receive_amount,
        stamp_duty_ngn: stampDutyNgn,
        stamp_duty_reimbursed: reimburse.ok,
        status: 'COMPLETED',
        transaction_id: result.transaction_id
    };
}

// ============================================
// HANDLERS — VOUCHERS
// ============================================
async function handleVoucherGenerate(packet) {
    const { from_wallet, code, expires_at, max_uses, to_wallet } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    if (!code) return { success: false, error: 'Code required' };

    const expiresAt = expires_at || Date.now() + 90 * 24 * 60 * 60 * 1000;

    await dbRun(
        `INSERT OR REPLACE INTO vouchers (code, amount, token, expires_at, max_uses, used_count, created_by, created_at, active, to_wallet)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [code.toUpperCase(), 0, 'REGISTRATION', expiresAt, max_uses || 1, 0, from_wallet, Date.now(), 1, to_wallet || null]
    );

    await addToLedger({
        type: 'VOUCHER_GENERATE',
        from: from_wallet,
        to: to_wallet || 'SYSTEM',
        amount: 0,
        token: 'REGISTRATION',
        extra: {
            code: code.toUpperCase(),
            seat_number: packet.seat_number,
            category: packet.category,
            voucher_type: packet.voucher_type || 'registration_proof',
            expires_at: expiresAt,
            max_uses: max_uses || 1,
            used_count: 0,
            active: 1,
            created_by: from_wallet,
            created_at: Date.now(),
            to_wallet: to_wallet || null
        },
        debit: false,
        credit: false
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
        type: 'VOUCHER_REDEEMED',
        from: from_wallet,
        to: voucher.created_by,
        amount: 0,
        token: 'REGISTRATION',
        extra: {
            code: code.toUpperCase(),
            redeemed_by: from_wallet,
            redeemed_at: Date.now(),
            used_count: voucher.used_count + 1
        },
        debit: false,
        credit: false
    });
    return { success: true };
}

// ============================================
// HANDLERS — MEMBER STATUS
// ============================================
async function handleFreeze(packet) {
    const { from_wallet, to_wallet } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    await dbRun(`UPDATE members SET status = 'frozen' WHERE wallet = ?`, [to_wallet]);
    await addToLedger({ type: 'FREEZE', from: from_wallet, to: to_wallet, amount: 0, token: 'RGT', debit: false, credit: false });
    return { success: true };
}

async function handleUnfreeze(packet) {
    const { from_wallet, to_wallet } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    await dbRun(`UPDATE members SET status = 'active' WHERE wallet = ?`, [to_wallet]);
    await addToLedger({ type: 'UNFREEZE', from: from_wallet, to: to_wallet, amount: 0, token: 'RGT', debit: false, credit: false });
    return { success: true };
}

async function handleBlacklist(packet) {
    const { from_wallet, to_wallet } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    await dbRun(`UPDATE members SET status = 'blacklisted' WHERE wallet = ?`, [to_wallet]);
    await addToLedger({ type: 'BLACKLIST', from: from_wallet, to: to_wallet, amount: 0, token: 'RGT', debit: false, credit: false });
    return { success: true };
}

async function handleUnblacklist(packet) {
    const { from_wallet, to_wallet } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };
    await dbRun(`UPDATE members SET status = 'active' WHERE wallet = ?`, [to_wallet]);
    await addToLedger({ type: 'UNBLACKLIST', from: from_wallet, to: to_wallet, amount: 0, token: 'RGT', debit: false, credit: false });
    return { success: true };
}

// ============================================
// HANDLERS — BOTS
// ============================================
async function handleFarmingPenalty(packet) {
    const { from_wallet, to_wallet, amount, destination } = packet;
    if (!await isAdmin(from_wallet)) return { success: false, error: 'Unauthorized' };

    await updateBalance(to_wallet, -amount, 'RGT');
    if (destination === 'vault') await updateBalance(WALLET_IDS.ADMIN, amount, 'RGT');
    else await updateBalance(WALLET_IDS.CROWN_BANK, amount, 'RGT');

    await dbRun(`INSERT INTO penalty_vault (wallet, amount, reason, date) VALUES (?, ?, ?, ?)`, [to_wallet, amount, 'Farming abuse', Date.now()]);
    await addToLedger({ type: 'FARMING_PENALTY', from: to_wallet, to: destination || 'VAULT', amount, token: 'RGT', debit: false, credit: false });
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

    await addToLedger({
        type: 'SUBSCRIPTION_BOT',
        from: from_wallet, to: 'SYSTEM',
        amount: total || total_extracted || 0,
        token: 'RGT',
        extra: { count: count || users_processed || 0, users: extracted_list || [] },
        debit: false, credit: false
    });
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

    await addToLedger({
        type: 'FARMING_BOT',
        from: from_wallet, to: 'SYSTEM',
        amount: total || total_penalty || 0,
        token: 'RGT',
        extra: { count: count || penalties_applied || 0, penalized: penalized_list || [] },
        debit: false, credit: false
    });
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
            await dbRun(`INSERT INTO penalty_vault (wallet, amount, reason, date) VALUES (?, ?, ?, ?)`, [entry.wallet, entry.amount, 'Suspicious activity', Date.now()]);
            count++; total += entry.amount;
        }
    }

    await addToLedger({
        type: 'SUSPICIOUS_BOT',
        from: from_wallet, to: 'SYSTEM',
        amount: total || total_penalty || 0,
        token: 'RGT',
        extra: { count: count || penalties_applied || 0, penalized: penalized_list || [] },
        debit: false, credit: false
    });
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

    await addToLedger({
        type: 'MASS_COLLECT',
        from: from_wallet, to: 'VAULT',
        amount: total || total_collected || 0,
        token: 'RGT',
        extra: { count: count || users_affected || 0, collected: collected_list || [] },
        debit: false, credit: false
    });
    return { success: true, processed: count || users_affected, total };
}

// ============================================
// HANDLERS — NOTES
// ============================================
async function handleNoteCreate(packet) {
    const { from_wallet, title, content, noteId } = packet;
    if (!content) return { success: false, error: 'Content required' };

    const id = noteId || 'NOTE_' + Date.now();
    await dbRun(`INSERT OR REPLACE INTO notes (id, wallet, title, content, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
        [id, from_wallet, title || content.substring(0, 40), content, Date.now(), Date.now()]);

    await addToLedger({ type: 'NEURAL_NOTE_CREATED', from: from_wallet, to: 'NOTEBOOK', amount: 0, token: 'RGT', extra: { title, content, noteId: id }, debit: false, credit: false });
    return { success: true, noteId: id };
}

async function handleNoteUpdate(packet) { return await handleNoteCreate(packet); }

async function handleNoteDelete(packet) {
    const { from_wallet, note_id, noteId } = packet;
    const id = note_id || noteId;
    await dbRun(`DELETE FROM notes WHERE id = ?`, [id]);
    await addToLedger({ type: 'NEURAL_NOTE_DELETED', from: from_wallet, to: 'NOTEBOOK', amount: 0, token: 'RGT', extra: { noteId: id }, debit: false, credit: false });
    return { success: true };
}

async function handleNotesCleared(packet) {
    await dbRun(`DELETE FROM notes WHERE wallet = ?`, [packet.from_wallet]);
    await addToLedger({ type: 'NEURAL_NOTES_CLEARED', from: packet.from_wallet, to: 'NOTEBOOK', amount: 0, token: 'RGT', debit: false, credit: false });
    return { success: true };
}

async function handleNotesSync(packet) { return { success: true, note_count: packet.note_count || 0 }; }

// ============================================
// HANDLERS — RECYCLE BIN
// ============================================
async function handleRecycleEvent(packet) {
    await addToLedger({
        type: packet.type || 'RECYCLE_EVENT',
        from: packet.from_wallet || 'ADMIN',
        to: 'SYSTEM',
        amount: 0,
        token: 'RGT',
        extra: packet,
        debit: false,
        credit: false
    });
    return { success: true };
}

// ============================================
// HANDLERS — SYNC
// ============================================
async function handlePendingSync(packet) {
    await dbRun(`INSERT INTO sync_queue (tx_id, data, attempts, created_at, status) VALUES (?, ?, ?, ?, ?)`,
        ['pending_' + Date.now(), JSON.stringify(packet.pending || []), 0, Date.now(), 'pending_queue']);
    return { success: true };
}

async function handleStatsSync(packet) {
    await dbRun(`INSERT INTO sync_queue (tx_id, data, attempts, created_at, status) VALUES (?, ?, ?, ?, ?)`,
        ['stats_' + Date.now(), JSON.stringify(packet.stats || {}), 0, Date.now(), 'stats']);
    return { success: true };
}

async function handleRegistrySync(packet) {
    if (!await isAdmin(packet.from_wallet)) {
        return { success: false, error: 'Unauthorized — admin only' };
    }

    const registry = packet.registry || [];
    if (registry.length === 0) return { success: true, count: 0 };

    await addToLedger({
        type: 'REGISTRY_SYNC',
        from: packet.from_wallet || 'ADMIN',
        to: 'SYSTEM',
        amount: 0,
        token: 'RGT',
        extra: { count: registry.length, registry: registry },
        debit: false,
        credit: false
    });

    console.log(`✅ Registry synced to ledger: ${registry.length} rows`);
    return { success: true, count: registry.length };
}

// ============================================
// HANDLERS — TOKEN FACTORY
// ============================================
async function handleTokenFactory(packet) {
    if (!await isAdmin(packet.from_wallet)) {
        return { success: false, error: 'Unauthorized — admin only' };
    }

    await addToLedger({
        type: packet.type || 'TOKEN_FACTORY_EVENT',
        from: packet.from_wallet || 'ADMIN',
        to: 'VAULT',
        amount: packet.amount || 0,
        token: packet.token || 'TOKEN',
        extra: packet.data || {},
        debit: false,
        credit: false
    });
    return { success: true };
}

async function handleClientSessionOpen(packet) {
    await addToLedger({ type: 'CLIENT_SESSION_OPEN', from: packet.from_wallet, to: 'SYSTEM', amount: 0, token: 'RGT', extra: { sessionStarted: Date.now() }, debit: false, credit: false });
    return { success: true };
}

// ============================================
// HANDLERS — REGISTRIES
// ============================================
async function handleWorkRegister(packet) {
    const {
        work_type, title, creator_wallet, creator_name,
        co_creators, description, genre, language,
        duration, pages, release_date,
        isrc, isbn, imdb_id, script_id,
        file_hash, file_url, cover_url
    } = packet;

    if (!work_type || !title || !creator_wallet) {
        return { success: false, error: 'Type, title, and creator wallet required' };
    }

    const validTypes = ['song', 'film', 'book', 'play'];
    if (!validTypes.includes(work_type)) {
        return { success: false, error: 'Invalid work type' };
    }

    if (!await walletExists(creator_wallet)) {
        return { success: false, error: 'Creator wallet not found' };
    }

    const workId = 'WORK_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);
    const prefix = work_type.toUpperCase().slice(0, 3);
    const catalogId = 'RC-' + prefix + '-' + String(Date.now()).slice(-8);

    await dbRun(
        `INSERT INTO creative_works (
            id, catalog_id, work_type, title, creator_wallet, creator_name,
            co_creators, description, genre, language,
            duration, pages, release_date,
            isrc, isbn, imdb_id, script_id,
            file_hash, file_url, cover_url,
            status, registered_by, registered_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
            workId, catalogId, work_type, title, creator_wallet, creator_name || '',
            JSON.stringify(co_creators || []),
            description || '', genre || '', language || '',
            duration || 0, pages || 0, release_date || '',
            isrc || '', isbn || '', imdb_id || '', script_id || '',
            file_hash || '', file_url || '', cover_url || '',
            'registered', packet.from_wallet, Date.now()
        ]
    );

    await addToLedger({
        type: 'WORK_REGISTER',
        from: packet.from_wallet,
        to: 'CREATIVE_REGISTRY',
        amount: 0,
        token: 'RGT',
        extra: {
            work_id: workId,
            catalog_id: catalogId,
            work_type: work_type,
            title: title,
            creator_wallet: creator_wallet,
            creator_name: creator_name,
            co_creators: co_creators,
            genre: genre
        },
        debit: false,
        credit: false
    });

    return { success: true, work_id: workId, catalog_id: catalogId, work_type };
}

async function handleMerchRegister(packet) {
    const {
        merch_type, title, creator_wallet, creator_name,
        description, category, linked_work_id,
        price, token, stock, sizes, colors, materials,
        sku, image_url, image_urls
    } = packet;

    if (!merch_type || !title || !creator_wallet) {
        return { success: false, error: 'Merch type, title, and creator wallet required' };
    }

    const validTypes = ['apparel', 'accessory', 'print', 'digital', 'collectible', 'other'];
    if (!validTypes.includes(merch_type)) {
        return { success: false, error: 'Invalid merch type' };
    }

    if (!await walletExists(creator_wallet)) {
        return { success: false, error: 'Creator wallet not found' };
    }

    const merchId = 'MERCH_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);
    const catalogId = 'RC-MRC-' + String(Date.now()).slice(-8);

    await dbRun(
        `INSERT INTO merch_registry (
            id, catalog_id, merch_type, title, creator_wallet, creator_name,
            description, category, linked_work_id,
            price, token, stock, sizes, colors, materials, sku,
            image_url, image_urls,
            status, registered_by, registered_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
            merchId, catalogId, merch_type, title, creator_wallet, creator_name || '',
            description || '', category || '', linked_work_id || null,
            price || 0, token || 'RGT', stock || 0,
            JSON.stringify(sizes || []),
            JSON.stringify(colors || []),
            materials || '', sku || '',
            image_url || null,
            JSON.stringify(image_urls || []),
            'registered', packet.from_wallet, Date.now()
        ]
    );

    await addToLedger({
        type: 'MERCH_REGISTER',
        from: packet.from_wallet,
        to: 'MERCH_REGISTRY',
        amount: 0,
        token: 'RGT',
        extra: {
            merch_id: merchId,
            catalog_id: catalogId,
            merch_type: merch_type,
            title: title,
            creator_wallet: creator_wallet,
            creator_name: creator_name,
            linked_work_id: linked_work_id || null,
            price: price || 0,
            token: token || 'RGT',
            stock: stock || 0
        },
        debit: false,
        credit: false
    });

    return { success: true, merch_id: merchId, catalog_id: catalogId, merch_type };
}

// ============================================
// ROUTER
// ============================================
async function routePacket(packet) {
    try {
        const check = await validatePacket(packet);
        if (!check.valid) {
            console.log(`❌ Rejected: ${check.error} | type=${packet?.type}`);
            return { success: false, error: check.error };
        }

        if (!packet.from_wallet && packet.type !== 'REGISTRATION_REQUEST') packet.from_wallet = 'UNKNOWN';

        const type = packet.type.toUpperCase();
        let result;

        switch (type) {
            case 'REGISTRATION_REQUEST':      result = await handleRegistrationRequest(packet); break;
            case 'USER_REGISTERED':           result = await handleUserRegistration(packet); break;
            case 'WALLET_DELETED':            result = await handleWalletDeleted(packet); break;
            case 'RATES_UPDATED':             result = await handleRatesUpdated(packet); break;

            case 'TRANSFER':                  result = await handleTransfer(packet); break;
            case 'VAULT_SEND':                result = await handleVaultSend(packet); break;
            case 'VAULT_RECEIVE':             result = await handleVaultReceive(packet); break;
            case 'P2P_TRANSFER':              result = await handlePeerTransfer(packet); break;
            case 'PEER_SETTLEMENT':           result = await handlePeerTransfer(packet); break;
            case 'MASS_PAY':                  result = await handleMassPay(packet); break;
            case 'MASS_PAY_BATCH':            result = await handleMassPayBatch(packet); break;
            case 'SWAP':                      result = await handleSwap(packet); break;
            case 'PURCHASE':                  result = await handlePurchase(packet); break;
            case 'PRODUCT_INTEREST':          result = await handleProductInterest(packet); break;
            case 'EVENT_ATTEND':              result = await handleEventAttend(packet); break;
            case 'STREAM_REWARD':             result = await handleStreamReward(packet); break;
            case 'STREAM_RATES_UPDATED':      result = await handleStreamRatesUpdated(packet); break;

            case 'MEDIA_UPLOAD':              result = await handleMediaUpload(packet); break;
            case 'MEDIA_RELEASED':            result = await handleMediaReleased(packet); break;
            case 'MEDIA_STREAM_EARNING':      result = await handleStreamReward(packet); break;
            case 'MEDIA_PAYOUT':              result = await handleStreamReward(packet); break;
            case 'MEDIA_MASTER_PAYOUT':       result = await handleStreamReward(packet); break;

            case 'BROADCAST_MSG':             result = await handleBroadcast(packet); break;
            case 'BOT_REACH':                 result = await handleBotReach(packet); break;
            case 'FEED_POST':                 result = await handleFeedPost(packet); break;
            case 'FEED_POST_CREATED':         result = await handleFeedPostCreated(packet); break;
            case 'FEED_INTERACTION':          result = await handleFeedInteraction(packet); break;
            case 'P2P_MSG':                   result = await handleP2PMessage(packet); break;
            case 'MASS_MSG':                  result = await handleMassMessage(packet); break;

            case 'NFT_MINT':                  result = await handleNFTMint(packet); break;
            case 'SHARE_CERTIFICATE_ISSUED':  result = await handleShareCertificateIssued(packet); break;
            case 'NFT_PURCHASE':              result = await handleNFTPurchase(packet); break;
            case 'NFT_SHARE_PURCHASE':        result = await handleNFTSharePurchase(packet); break;
            case 'SHARE_PURCHASE':            result = await handleSharePurchase(packet); break;

            case 'CASHOUT_REQUEST':           result = await handleCashoutRequest(packet); break;
            case 'CASH_OUT':                  result = await handleCashOut(packet); break;
            case 'CASHOUT_APPROVED':          result = await handleCashoutApproved(packet); break;
            case 'CASHOUT_REJECTED':          result = await handleCashoutRejected(packet); break;
            case 'BANKING_SYNC':              result = await handleBankingSync(packet); break;
            case 'PROCESSOR_CONNECTED':       result = await handleProcessorConnected(packet); break;
            case 'POLICY_UPDATED':            result = await handlePolicyUpdated(packet); break;

            case 'PURCHASE_QUOTE':            result = await handlePurchaseQuote(packet); break;
            case 'PURCHASE_EXECUTE':          result = await handlePurchaseExecute(packet); break;

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

            case 'NOTE_CREATE':
            case 'NEURAL_NOTE_CREATED':       result = await handleNoteCreate(packet); break;
            case 'NEURAL_NOTE_UPDATED':       result = await handleNoteUpdate(packet); break;
            case 'NOTE_DELETE':
            case 'NEURAL_NOTE_DELETED':       result = await handleNoteDelete(packet); break;
            case 'NEURAL_NOTES_CLEARED':      result = await handleNotesCleared(packet); break;
            case 'NOTES_SYNC':                result = await handleNotesSync(packet); break;

            case 'PRUNE_RULES_SAVED':
            case 'PRUNE_EXECUTED':
            case 'RESTORE_EXECUTED':
            case 'BIN_EMPTIED':               result = await handleRecycleEvent(packet); break;

            case 'PENDING_SYNC':              result = await handlePendingSync(packet); break;
            case 'STATS_SYNC':                result = await handleStatsSync(packet); break;
            case 'REGISTRY_SYNC':             result = await handleRegistrySync(packet); break;

            case 'TOKEN_FACTORY_FACTORY_SAVED':
            case 'TOKEN_FACTORY_TOKEN_MINTED':
            case 'TOKEN_FACTORY_WEEKLY_WALLETS_UPDATED':
            case 'TOKEN_FACTORY_WEEKLY_AMOUNT_SET':
            case 'TOKEN_FACTORY_WEEKLY_MINT_EXECUTED':
            case 'TOKEN_MINT':                result = await handleTokenFactory(packet); break;

            case 'WORK_REGISTER':             result = await handleWorkRegister(packet); break;
            case 'MERCH_REGISTER':            result = await handleMerchRegister(packet); break;

            case 'CLIENT_SESSION_OPEN':       result = await handleClientSessionOpen(packet); break;

            default:
                result = { success: true, message: 'Pass-through', type };
        }
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
    pingTimeout: 60000,
    pingInterval: 25000,
    transports: ['websocket', 'polling']
});

app.use(helmet({ crossOriginResourcePolicy: { policy: 'cross-origin' }, contentSecurityPolicy: false }));
app.use(cors({ origin: CORS_ORIGIN }));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

const ROOT = __dirname;
const FILE_MAP = {
    '/dashboard': 'index.html',
    '/control': 'server-control.html',
    '/financial': 'financial.html',
    '/banking': 'banking-api.html',
    '/nft': 'nft-minter.html',
    '/neural': 'neural-chain.html',
    '/storage': 'storage-vault.html',
    '/recycling': 'recycle-bin.html',
    '/onboarding': 'onboarding.html',
    '/tokenfactory': 'token-factory.html',
    '/streaming': 'streaming-royalty.html',
    '/autodeduction': 'auto-deduction.html',
    '/notebook': 'notepad.html',
    '/cover': 'cover.html',
    '/client': 'dashboard.html',
    '/correspondence': 'correspondence.html',
    '/voucher': 'voucher.html',
    '/works': 'work registry.html',
    '/merch': 'merch registry.html'
};

Object.entries(FILE_MAP).forEach(([route, filename]) => {
    app.get(route, (req, res) => {
        const full = path.join(ROOT, filename);
        if (fs.existsSync(full)) res.sendFile(full);
        else res.status(404).send(`File not found: ${filename}`);
    });
});

app.get('/*.html', (req, res) => {
    const full = path.join(ROOT, path.basename(req.path));
    if (fs.existsSync(full)) res.sendFile(full);
    else res.status(404).send('Not found');
});

const serverState = {
    startedAt: Date.now(),
    packetCount: 0,
    syncCount: 0,
    heartbeatCount: 0,
    fallbackModeActive: false,
    lastHeartbeat: null,
    lastHeartbeatTx: null,
    lastSyncTime: null
};

// ============================================
// API — HEALTH & STATUS
// ============================================
app.get('/health', (req, res) => {
    res.json({
        status: 'ok', systemId: SYSTEM_ID,
        role: IS_CLOUD ? 'cloud' : 'desktop',
        platform: 'node', node: process.version,
        uptime: Math.floor((Date.now() - serverState.startedAt) / 1000),
        timestamp: Date.now()
    });
});

app.get('/status', async (req, res) => {
    try {
        const pendingRow = await dbGet(`SELECT COUNT(*) as c FROM sync_queue WHERE status = 'pending'`);
        res.json({
            success: true,
            uptime: Math.floor((Date.now() - serverState.startedAt) / 1000),
            packets: serverState.packetCount, synced: serverState.syncCount,
            pending: pendingRow ? pendingRow.c : 0,
            heartbeatCount: serverState.heartbeatCount,
            fallbackMode: serverState.fallbackModeActive,
            lastHeartbeat: serverState.lastHeartbeat,
            lastHeartbeatTx: serverState.lastHeartbeatTx,
            lastSync: serverState.lastSyncTime,
            systemId: SYSTEM_ID, role: IS_CLOUD ? 'cloud' : 'desktop'
        });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ============================================
// API — PACKET
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
                type: packet.type, from: packet.from_wallet,
                to: packet.to_wallet, result
            });
        }
        res.json(result);
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});

// ============================================
// API — PURCHASE
// ============================================
app.post('/api/purchase/quote', async (req, res) => {
    try {
        const packet = { type: 'PURCHASE_QUOTE', from_wallet: 'ADMIN_VAULT', ...req.body };
        const result = await routePacket(packet);
        res.json(result);
    } catch (error) {
        res.status(400).json({ ok: false, message: error.message });
    }
});

app.post('/api/purchase/execute', async (req, res) => {
    try {
        const packet = { type: 'PURCHASE_EXECUTE', from_wallet: 'ADMIN_VAULT', ...req.body };
        const result = await routePacket(packet);
        res.json(result);
    } catch (error) {
        res.status(400).json({ ok: false, message: error.message });
    }
});

// ============================================
// API — READS
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
    try {
        const works = await dbAll('SELECT * FROM creative_works ORDER BY registered_at DESC');
        res.json({ success: true, count: works.length, works });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});

app.get('/api/merch', async (req, res) => {
    try {
        const merch = await dbAll('SELECT * FROM merch_registry ORDER BY registered_at DESC');
        res.json({ success: true, count: merch.length, merch });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});

app.get('/api/registrations/pending', async (req, res) => {
    try {
        const rows = await dbAll(`SELECT * FROM pending_registrations WHERE status = 'pending' ORDER BY submitted_at DESC`);
        res.json({ success: true, count: rows.length, registrations: rows });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
});

app.get('/api/cashouts/:id', async (req, res) => {
    try {
        const cashout = await dbGet('SELECT * FROM pending_cashouts WHERE id = ?', [req.params.id]);
        if (!cashout) return res.json({ ok: false, message: 'Cash request not found.' });
        res.json({
            ok: true,
            id: cashout.id,
            wallet: cashout.wallet,
            amount: cashout.amount,
            status: cashout.status
        });
    } catch (error) {
        res.status(400).json({ ok: false, message: error.message });
    }
});

// ============================================
// API — ADMIN
// ============================================
app.post('/api/admin/rebuild', async (req, res) => {
    try {
        const result = await rebuildStateFromLedger();
        res.json(result);
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// ============================================
// API — SERVER CONTROL
// ============================================
app.post('/api/start', (req, res) => {
    res.json({ success: true, message: 'Server is running', uptime: Math.floor((Date.now() - serverState.startedAt) / 1000) });
});
app.post('/api/shutdown', (req, res) => {
    res.json({ success: true, message: 'Shutting down' });
    setTimeout(async () => {
        try {
            if (!IS_CLOUD) { await pullFromFallback(); await pushBacklogToFallback(); }
        } catch (err) { console.warn('⚠️ Final sync failed:', err.message); }
        process.exit(0);
    }, 1500);
});
app.post('/api/restart', (req, res) => {
    res.json({ success: true, message: 'Restarting...' });
    setTimeout(() => process.exit(0), 1000);
});
app.post('/api/fallback/activate', (req, res) => {
    serverState.fallbackModeActive = true;
    res.json({ success: true, fallbackMode: true });
});
app.post('/api/fallback/deactivate', (req, res) => {
    serverState.fallbackModeActive = false;
    res.json({ success: true, fallbackMode: false });
});
app.post('/api/sync', async (req, res) => {
    try {
        if (IS_CLOUD) return res.json({ success: true, count: 0, message: 'Cloud' });
        const pulled = await pullFromFallback();
        const pushed = await pushBacklogToFallback();
        serverState.syncCount = (serverState.syncCount || 0) + pulled + pushed;
        serverState.lastSyncTime = Date.now();
        res.json({ success: true, count: pulled + pushed, pulled, pushed });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ============================================
// HEARTBEAT
// ============================================
let heartbeatIntervalHandle = null;
let heartbeatAuto = false;

async function sendHeartbeatToBSC() {
    if (IS_CLOUD) return { success: false, error: 'Cloud does not sign heartbeats' };
    if (!BSC_PRIVATE_KEY) return { success: false, error: 'Private key not configured' };

    try {
        const provider = new ethers.JsonRpcProvider(BSC_RPC_URL);
        const wallet = new ethers.Wallet(BSC_PRIVATE_KEY, provider);

        const HEARTBEAT_ABI = [
            'function sendHeartbeat(bytes32 _stateHash, uint256 _walletCount) external',
            'function isDesktopAlive() view returns (bool)',
            'function lastHeartbeat() view returns (uint256 timestamp, bytes32 stateHash, uint256 walletCount, bool alive)'
        ];
        const contract = new ethers.Contract(BSC_CONTRACT_ADDRESS, HEARTBEAT_ABI, wallet);

        const recent = await dbAll('SELECT tx_id, type, timestamp FROM ledger ORDER BY timestamp DESC LIMIT 100');
        const stateHash = ethers.keccak256(ethers.toUtf8Bytes(JSON.stringify(recent)));
        const row = await dbGet('SELECT COUNT(*) as c FROM members');
        const walletCount = row ? row.c : 0;

        const tx = await contract.sendHeartbeat(stateHash, walletCount);
        const receipt = await tx.wait();

        serverState.heartbeatCount++;
        serverState.lastHeartbeat = Date.now();
        serverState.lastHeartbeatTx = tx.hash;

        await dbRun(`INSERT INTO heartbeat_log (tx_hash, state_hash, wallet_count, timestamp, status) VALUES (?, ?, ?, ?, ?)`,
            [tx.hash, stateHash, walletCount, Date.now(), 'confirmed']);

        console.log(`✅ Heartbeat: ${tx.hash}`);
        return { success: true, hash: tx.hash, block: receipt.blockNumber };
    } catch (err) {
        console.error('❌ Heartbeat error:', err.message);
        return { success: false, error: err.message };
    }
}

app.post('/api/heartbeat', async (req, res) => { res.json(await sendHeartbeatToBSC()); });
app.get('/api/heartbeat/status', async (req, res) => {
    try {
        if (IS_CLOUD || !BSC_PRIVATE_KEY) return res.json({ success: true, alive: false, error: IS_CLOUD ? 'Cloud' : 'Not configured' });
        const provider = new ethers.JsonRpcProvider(BSC_RPC_URL);
        const ABI = ['function isDesktopAlive() view returns (bool)'];
        const contract = new ethers.Contract(BSC_CONTRACT_ADDRESS, ABI, provider);
        const alive = await contract.isDesktopAlive();
        res.json({ success: true, alive, lastHash: serverState.lastHeartbeatTx, heartbeatCount: serverState.heartbeatCount });
    } catch (err) { res.json({ success: true, alive: false, error: err.message }); }
});
app.post('/api/heartbeat/auto/start', (req, res) => {
    if (IS_CLOUD) return res.json({ success: false, error: 'Cloud' });
    if (heartbeatAuto) return res.json({ success: true, message: 'Already running' });
    heartbeatAuto = true;
    heartbeatIntervalHandle = setInterval(sendHeartbeatToBSC, 60000);
    sendHeartbeatToBSC();
    res.json({ success: true, message: 'Auto heartbeat started' });
});
app.post('/api/heartbeat/auto/stop', (req, res) => {
    if (heartbeatIntervalHandle) clearInterval(heartbeatIntervalHandle);
    heartbeatIntervalHandle = null; heartbeatAuto = false;
    res.json({ success: true, message: 'Auto heartbeat stopped' });
});

// ============================================
// SYNC
// ============================================
async function pullFromFallback() {
    if (IS_CLOUD) return 0;
    try {
        const response = await fetch(`${FALLBACK_SERVER_URL}/api/ledger?limit=200`, { signal: AbortSignal.timeout(8000) });
        if (!response.ok) throw new Error('Fallback unreachable');

        const data = await response.json();
        const entries = data.ledger || [];

        let inserted = 0;
        for (const e of entries) {
            const existing = await dbGet('SELECT tx_id FROM ledger WHERE tx_id = ?', [e.tx_id]);
            if (!existing) {
                await dbRun(
                    `INSERT INTO ledger (tx_id, type, from_wallet, to_wallet, amount, token, timestamp, status, extra, sync_status)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [e.tx_id, e.type, e.from_wallet, e.to_wallet, e.amount, e.token, e.timestamp, e.status || 'confirmed', e.extra || '{}', 'synced']
                );
                await applyLedgerEntry({
                    tx_id: e.tx_id, type: e.type,
                    from_wallet: e.from_wallet, to_wallet: e.to_wallet,
                    amount: e.amount, token: e.token,
                    timestamp: e.timestamp, extra: e.extra
                });
                inserted++;
            }
        }
        if (inserted > 0) console.log(`📥 Pulled ${inserted} entries from fallback`);
        return inserted;
    } catch (err) {
        console.warn('⚠️ Pull failed:', err.message);
        return 0;
    }
}

async function pushBacklogToFallback() {
    if (IS_CLOUD) return 0;
    try {
        const pending = await dbAll(`SELECT * FROM sync_queue WHERE status = 'pending' LIMIT 100`);
        let pushed = 0;
        for (const item of pending) {
            try {
                const entry = JSON.parse(item.data);
                const response = await fetch(`${FALLBACK_SERVER_URL}/api/sync/ledger`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        tx_id: item.tx_id, type: entry.type,
                        from_wallet: entry.from || entry.from_wallet,
                        to_wallet: entry.to || entry.to_wallet,
                        amount: entry.amount, token: entry.token,
                        timestamp: Date.now(), extra: entry.extra || {}
                    }),
                    signal: AbortSignal.timeout(8000)
                });
                if (response.ok) {
                    await dbRun(`UPDATE sync_queue SET status = 'synced' WHERE id = ?`, [item.id]);
                    pushed++;
                }
            } catch (err) {
                await dbRun(`UPDATE sync_queue SET attempts = attempts + 1 WHERE id = ?`, [item.id]);
            }
        }
        if (pushed > 0) console.log(`📤 Pushed ${pushed} backlog entries`);
        return pushed;
    } catch (err) { return 0; }
}

app.post('/api/sync/ledger', async (req, res) => {
    if (!IS_CLOUD) return res.json({ success: true, message: 'Desktop does not accept sync' });
    try {
        const input = req.body;
        if (!input || !input.tx_id) return res.status(400).json({ success: false, error: 'tx_id required' });

        const existing = await dbGet('SELECT tx_id FROM ledger WHERE tx_id = ?', [input.tx_id]);
        if (existing) return res.json({ success: true, message: 'Already have', tx_id: input.tx_id });

        await dbRun(
            `INSERT INTO ledger (tx_id, type, from_wallet, to_wallet, amount, token, timestamp, status, extra, sync_status)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [input.tx_id, input.type, input.from_wallet, input.to_wallet, input.amount, input.token,
             input.timestamp || Date.now(), 'confirmed', JSON.stringify(input.extra || {}), 'synced']
        );

        await applyLedgerEntry({
            tx_id: input.tx_id, type: input.type,
            from_wallet: input.from_wallet, to_wallet: input.to_wallet,
            amount: input.amount, token: input.token,
            timestamp: input.timestamp, extra: input.extra
        });

        if (io) {
            io.emit('ledger_entry', {
                tx_id: input.tx_id, type: input.type,
                from: input.from_wallet, to: input.to_wallet,
                amount: input.amount, token: input.token,
                timestamp: input.timestamp || Date.now(), extra: input.extra
            });
        }

        res.json({ success: true, tx_id: input.tx_id });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ============================================
// WEBSOCKET
// ============================================
io.on('connection', (socket) => {
    console.log(`🔌 WS connected: ${socket.id}`);
    socket.on('authenticate', (wallet) => {
        if (wallet) {
            socket.wallet = wallet;
            console.log(`🔐 Auth: ${wallet}`);
            socket.emit('authenticated', { success: true, wallet });
        }
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
                io.emit('update', { type: packet.type, from: packet.from_wallet, to: packet.to_wallet, result });
            }
        } catch (err) { socket.emit('error', { error: err.message }); }
    });
    socket.on('disconnect', () => { console.log(`🔌 WS disconnected: ${socket.id}`); });
});

// ============================================
// BACKGROUND LOOPS
// ============================================
setInterval(async () => {
    if (!IS_CLOUD) { await pullFromFallback(); await pushBacklogToFallback(); }
}, 60000);

setInterval(async () => {
    if (!IS_CLOUD && BSC_PRIVATE_KEY) await sendHeartbeatToBSC();
}, 60000);

// ============================================
// STARTUP
// ============================================
async function startup() {
    console.log('═══════════════════════════════════════');
    console.log(`🚀 RC RECORDS SERVER (${SYSTEM_ID}) — Node ${process.version}`);
    console.log(`   Role: ${IS_CLOUD ? '☁️  CLOUD FALLBACK' : '🖥️  DESKTOP PRIMARY'}`);
    console.log('═══════════════════════════════════════');

    if (!IS_CLOUD) {
        await pullFromFallback();
        await pushBacklogToFallback();

        const ledgerCount = await dbGet('SELECT COUNT(*) as c FROM ledger');
        const memberCount = await dbGet('SELECT COUNT(*) as c FROM members');
        console.log(`   Ledger: ${ledgerCount.c} entries | Members: ${memberCount.c} records`);

        if (ledgerCount.c > 0 && memberCount.c === 0) {
            console.log('⚠️ Ledger has entries but members table empty — rebuilding...');
            await rebuildStateFromLedger();
        }

        if (BSC_PRIVATE_KEY) {
            console.log('💓 Sending initial heartbeat...');
            await sendHeartbeatToBSC();
        } else {
            console.log('⚠️ BSC_PRIVATE_KEY not set — heartbeat disabled');
        }
    } else {
        console.log('☁️  Cloud mode');
    }

    console.log('═══════════════════════════════════════');
    console.log(`   System:    ${SYSTEM_ID}`);
    console.log(`   Port:      ${PORT}`);
    console.log(`   Admin:     http://localhost:${PORT}/dashboard`);
    console.log('═══════════════════════════════════════');
}

server.listen(PORT, '0.0.0.0', startup);

// ============================================
// GRACEFUL SHUTDOWN
// ============================================
process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

async function gracefulShutdown() {
    console.log('\n🛑 Shutdown signal received');
    try {
        if (!IS_CLOUD) { await pullFromFallback(); await pushBacklogToFallback(); }
    } catch (err) { console.warn('⚠️ Final sync failed:', err.message); }
    db.close(() => server.close(() => process.exit(0)));
}

process.on('uncaughtException', (err) => console.error('❌ Uncaught exception:', err.message));
process.on('unhandledRejection', (reason) => console.error('❌ Unhandled rejection:', reason));

module.exports = { app, server, io, db, routePacket };
