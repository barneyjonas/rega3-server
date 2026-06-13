import express from 'express'
import { createServer } from 'http'
import { WebSocketServer, WebSocket } from 'ws'
import cors from 'cors'
import { v4 as uuidv4 } from 'uuid'
import db from './db'

const app = express()
app.use(cors())
app.use(express.json({ limit: '10mb' }))

const httpServer = createServer(app)
const wss = new WebSocketServer({ server: httpServer })

// userId -> WebSocket
const online = new Map<string, WebSocket>()

// ── Types ──────────────────────────────────────────────────────────────

interface WsMessage {
  type: string
  [key: string]: unknown
}

interface DbUser {
  id: string
  username: string
  display_name: string
  created_at: number
}

interface DbConversation {
  id: string
  created_at: number
}

interface DbMessage {
  id: string
  conversation_id: string
  sender_id: string
  type: string
  text: string
  voice_uri: string | null
  voice_duration: number | null
  voice_waveform: string | null
  voice_segments: string | null
  timestamp: number
  status: string
}

// ── Helpers ────────────────────────────────────────────────────────────

function send(ws: WebSocket, data: object) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data))
}

function broadcast(userIds: string[], data: object, exceptUserId?: string) {
  for (const uid of userIds) {
    if (uid === exceptUserId) continue
    const ws = online.get(uid)
    if (ws) send(ws, data)
  }
}

function getConvMembers(conversationId: string): string[] {
  const rows = db.prepare('SELECT user_id FROM conversation_members WHERE conversation_id = ?').all(conversationId) as { user_id: string }[]
  return rows.map((r) => r.user_id)
}

function formatMessage(row: DbMessage) {
  return {
    id: row.id,
    conversation_id: row.conversation_id,
    sender_id: row.sender_id,
    type: row.type,
    text: row.text,
    voice_uri: row.voice_uri,
    voice_duration: row.voice_duration,
    voice_waveform: row.voice_waveform ? JSON.parse(row.voice_waveform) : undefined,
    voice_segments: row.voice_segments ? JSON.parse(row.voice_segments) : undefined,
    timestamp: row.timestamp,
    status: row.status,
  }
}

// ── REST API ────────────────────────────────────────────────────────────

// Register or login (simple: just username, no password for now)
app.post('/auth/register', (req, res) => {
  const { username, display_name } = req.body as { username: string; display_name: string }
  if (!username || !display_name) return res.status(400).json({ error: 'username and display_name required' })

  const existing = db.prepare('SELECT * FROM users WHERE username = ?').get(username) as DbUser | undefined
  if (existing) return res.json({ user: existing })

  const user: DbUser = { id: uuidv4(), username, display_name, created_at: Date.now() }
  db.prepare('INSERT INTO users (id, username, display_name, created_at) VALUES (?, ?, ?, ?)').run(user.id, user.username, user.display_name, user.created_at)
  res.json({ user })
})

// Get all users (for starting conversations)
app.get('/users', (_req, res) => {
  const users = db.prepare('SELECT id, username, display_name FROM users').all()
  res.json({ users })
})

// Get or create a 1-on-1 conversation between two users
app.post('/conversations/direct', (req, res) => {
  const { user_a, user_b } = req.body as { user_a: string; user_b: string }
  if (!user_a || !user_b) return res.status(400).json({ error: 'user_a and user_b required' })

  // Find existing conversation with exactly these two members
  const existing = db.prepare(`
    SELECT cm1.conversation_id FROM conversation_members cm1
    JOIN conversation_members cm2 ON cm1.conversation_id = cm2.conversation_id
    WHERE cm1.user_id = ? AND cm2.user_id = ?
    AND (SELECT COUNT(*) FROM conversation_members WHERE conversation_id = cm1.conversation_id) = 2
  `).get(user_a, user_b) as { conversation_id: string } | undefined

  if (existing) {
    const conv = db.prepare('SELECT * FROM conversations WHERE id = ?').get(existing.conversation_id) as DbConversation
    return res.json({ conversation: conv })
  }

  const conv: DbConversation = { id: uuidv4(), created_at: Date.now() }
  db.prepare('INSERT INTO conversations (id, created_at) VALUES (?, ?)').run(conv.id, conv.created_at)
  db.prepare('INSERT INTO conversation_members (conversation_id, user_id) VALUES (?, ?)').run(conv.id, user_a)
  db.prepare('INSERT INTO conversation_members (conversation_id, user_id) VALUES (?, ?)').run(conv.id, user_b)
  res.json({ conversation: conv })
})

// Get conversations for a user
app.get('/users/:userId/conversations', (req, res) => {
  const { userId } = req.params
  const convIds = db.prepare('SELECT conversation_id FROM conversation_members WHERE user_id = ?').all(userId) as { conversation_id: string }[]

  const result = convIds.map(({ conversation_id }) => {
    const members = db.prepare(`
      SELECT u.id, u.username, u.display_name FROM users u
      JOIN conversation_members cm ON u.id = cm.user_id
      WHERE cm.conversation_id = ?
    `).all(conversation_id) as DbUser[]

    const lastMsg = db.prepare('SELECT * FROM messages WHERE conversation_id = ? ORDER BY timestamp DESC LIMIT 1').get(conversation_id) as DbMessage | undefined

    return {
      id: conversation_id,
      members,
      last_message: lastMsg ? formatMessage(lastMsg) : null,
    }
  })

  res.json({ conversations: result })
})

// Get messages for a conversation
app.get('/conversations/:convId/messages', (req, res) => {
  const { convId } = req.params
  const limit = Number(req.query.limit ?? 50)
  const before = Number(req.query.before ?? Date.now() + 1)

  const rows = db.prepare('SELECT * FROM messages WHERE conversation_id = ? AND timestamp < ? ORDER BY timestamp DESC LIMIT ?').all(convId, before, limit) as DbMessage[]
  res.json({ messages: rows.reverse().map(formatMessage) })
})

// Health check
app.get('/health', (_req, res) => res.json({ ok: true }))

// ── WebSocket ───────────────────────────────────────────────────────────

wss.on('connection', (ws) => {
  let userId: string | null = null

  ws.on('message', (raw) => {
    let msg: WsMessage
    try { msg = JSON.parse(raw.toString()) } catch { return }

    // Identify: client sends { type: 'identify', userId }
    if (msg.type === 'identify') {
      userId = msg.userId as string
      online.set(userId, ws)
      send(ws, { type: 'identified', userId })
      return
    }

    if (!userId) return

    // Send a message
    if (msg.type === 'send_message') {
      const { conversation_id, id, type, text, voice_uri, voice_duration, voice_waveform, voice_segments, timestamp } = msg as {
        conversation_id: string; id: string; type: string; text: string
        voice_uri?: string; voice_duration?: number; voice_waveform?: number[]; voice_segments?: unknown[]; timestamp: number
      }

      const msgId = id ?? uuidv4()
      db.prepare(`
        INSERT INTO messages (id, conversation_id, sender_id, type, text, voice_uri, voice_duration, voice_waveform, voice_segments, timestamp, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'sent')
      `).run(
        msgId, conversation_id, userId, type ?? 'text', text ?? '',
        voice_uri ?? null, voice_duration ?? null,
        voice_waveform ? JSON.stringify(voice_waveform) : null,
        voice_segments ? JSON.stringify(voice_segments) : null,
        timestamp ?? Date.now()
      )

      const saved = db.prepare('SELECT * FROM messages WHERE id = ?').get(msgId) as DbMessage
      const formatted = formatMessage(saved)

      // Confirm to sender
      send(ws, { type: 'message_saved', message: formatted })

      // Deliver to other members
      const members = getConvMembers(conversation_id)
      broadcast(members, { type: 'new_message', message: formatted }, userId)

      // Mark delivered to sender if recipient is online
      const others = members.filter((m) => m !== userId)
      const anyOnline = others.some((m) => online.has(m))
      if (anyOnline) {
        db.prepare('UPDATE messages SET status = ? WHERE id = ?').run('delivered', msgId)
        send(ws, { type: 'status_update', messageId: msgId, status: 'delivered' })
      }
    }

    // Mark messages as read
    if (msg.type === 'mark_read') {
      const { conversation_id } = msg as unknown as { conversation_id: string }
      db.prepare(`UPDATE messages SET status = 'read' WHERE conversation_id = ? AND sender_id != ? AND status != 'read'`).run(conversation_id, userId)

      const members = getConvMembers(conversation_id)
      broadcast(members, { type: 'messages_read', conversation_id, by: userId }, userId)
    }
  })

  ws.on('close', () => {
    if (userId) online.delete(userId)
  })
})

// ── Start ───────────────────────────────────────────────────────────────

const PORT = Number(process.env.PORT ?? 3000)
httpServer.listen(PORT, () => {
  console.log(`rega3 server running on port ${PORT}`)
})
