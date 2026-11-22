require('dotenv').config();
const express = require('express');
const path = require('path');
const rateLimit = require('express-rate-limit');

const app = express();
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.GEMINI_API_KEY;
const MODEL = process.env.GEMINI_MODEL || 'gemini-3.5-flash-lite';
const MAX_QUESTION_LENGTH = 500;

if (!API_KEY || API_KEY === 'your_gemini_api_key_here' || API_KEY === 'your-api-key-here') {
  console.error('Missing GEMINI_API_KEY. Add it to your .env file.');
  process.exit(1);
}

// Rate limiter: max 30 requests per 5 minutes per IP
const askLimiter = rateLimit({
  windowMs: 5 * 60 * 1000,
  max: 30,
  message: { error: 'Too many requests from this address. Please wait a few minutes before asking again.' },
  standardHeaders: true,
  legacyHeaders: false,
});

app.use(express.json({ limit: '10kb' }));
app.use(express.static(path.join(__dirname, 'public')));
app.use('/api/', askLimiter);

const SYSTEM_PROMPT = `
You are a warm, experienced pastor answering Bible questions for people who may be
new to Christianity or who have not attended church before. You are not tied to any
denomination. Speak in the shared core beliefs of historic Christianity and rely on
what the Bible itself says.

RULES
1. Only answer questions about the Christian faith, the Bible, or Christian living.
   If a question is off-topic, reply kindly in one or two sentences saying you can
   only help with questions about the Bible and the Christian faith, and invite them
   to ask one. Do not answer off-topic questions.
2. Base every answer on Scripture. Name the specific Bible references you rely on
   (book, chapter, verse) and briefly explain what they say. Do not invent verses.
   When quoting Scripture, write out the verse text using an easy-to-read modern translation
   (such as the NIV, ESV, or NLT) in a blockquote format (> "verse text"). Never use the King James Version (KJV).
   If the Bible does not directly address something, say so honestly and explain
   what it does teach about the topic.
3. Where Christians differ on secondary matters (for example, baptism methods,
   end-times views, or spiritual gifts), briefly say that believers hold different
   views, then focus on what the text clearly teaches and what most Christians agree on.
4. Write at a 5th-grade reading level:
   - Short sentences, common words.
   - Explain any church or theology word the first time you use it (for example,
     "grace means God's undeserved kindness").
   - Avoid jargon such as "eschatology," "hermeneutics," or "soteriology."
5. Keep answers short, about 150 to 250 words. Use this format:
   - A direct answer in the first one or two sentences.
   - Two to four short paragraphs explaining it with Scripture.
   - A final line starting with "Key verses:" that lists the references.
6. Tone: gentle, encouraging, and honest. Speak as a pastor who cares about the
   person. Do not lecture or shame. Do not claim to be a human being.
7. If someone shares pain, fear, or a crisis, respond with compassion, point them to
   Scripture's comfort, and encourage them to talk with a local pastor, a trusted
   Christian friend, or a doctor or counselor. If there is any risk of harm to
   themselves or others, encourage them to contact local emergency services right away.
8. Do not give medical, legal, or financial advice. Do not present yourself as a
   replacement for a local church, pastoral care, or a Bible study group. Gently
   encourage people to find a local church community.
`;

// Helper to build Gemini payload
function buildGeminiPayload(question) {
  return {
    system_instruction: {
      parts: [{ text: SYSTEM_PROMPT.trim() }],
    },
    contents: [
      {
        role: 'user',
        parts: [{ text: question }],
      },
    ],
    generationConfig: {
      maxOutputTokens: 900,
    },
  };
}

// Streaming endpoint with Server-Sent Events (SSE)
app.post('/api/ask-stream', async (req, res) => {
  const question = String(req.body?.question || '').trim();

  if (!question) {
    return res.status(400).json({ error: 'Please type a question first.' });
  }
  if (question.length > MAX_QUESTION_LENGTH) {
    return res
      .status(400)
      .json({ error: `Please keep your question under ${MAX_QUESTION_LENGTH} characters.` });
  }

  // Set SSE response headers
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
  });

  try {
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(MODEL)}:streamGenerateContent?alt=sse&key=${encodeURIComponent(API_KEY)}`;
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(buildGeminiPayload(question)),
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error('Gemini streaming API error:', response.status, errText);
      res.write(`event: error\ndata: ${JSON.stringify({ error: 'The answer service is unavailable right now. Please try again soon.' })}\n\n`);
      return res.end();
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop(); // keep last incomplete line

      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || !trimmed.startsWith('data:')) continue;
        const jsonStr = trimmed.slice(5).trim();
        if (!jsonStr || jsonStr === '[DONE]') continue;

        try {
          const parsed = JSON.parse(jsonStr);
          const candidate = parsed.candidates?.[0];
          const text = (candidate?.content?.parts || [])
            .map((p) => p.text || '')
            .join('');

          if (text) {
            res.write(`data: ${JSON.stringify({ text })}\n\n`);
          }
        } catch (e) {
          // ignore partial parse error
        }
      }
    }

    res.write('event: done\ndata: {}\n\n');
    res.end();
  } catch (err) {
    console.error('Server streaming error:', err);
    res.write(`event: error\ndata: ${JSON.stringify({ error: 'Something went wrong while generating the answer.' })}\n\n`);
    res.end();
  }
});

// Fallback non-streaming endpoint
app.post('/api/ask', async (req, res) => {
  const question = String(req.body?.question || '').trim();

  if (!question) {
    return res.status(400).json({ error: 'Please type a question first.' });
  }
  if (question.length > MAX_QUESTION_LENGTH) {
    return res
      .status(400)
      .json({ error: `Please keep your question under ${MAX_QUESTION_LENGTH} characters.` });
  }

  try {
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(MODEL)}:generateContent?key=${encodeURIComponent(API_KEY)}`;
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify(buildGeminiPayload(question)),
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error('Gemini API error:', response.status, errText);
      return res
        .status(502)
        .json({ error: 'The answer service is unavailable right now. Please try again soon.' });
    }

    const data = await response.json();
    const candidate = data.candidates?.[0];
    const answer = (candidate?.content?.parts || [])
      .map((part) => part.text || '')
      .join('\n')
      .trim();

    if (!answer) {
      const blockReason = candidate?.finishReason || data.promptFeedback?.blockReason;
      console.warn('Gemini empty answer. Details:', JSON.stringify({ blockReason, candidate, promptFeedback: data.promptFeedback }));
      return res
        .status(502)
        .json({
          error: blockReason
            ? `The response was filtered by the provider (${blockReason}). Please try rephrasing your question.`
            : 'No response was returned from the service. Please try again.',
        });
    }

    res.json({ answer });
  } catch (err) {
    console.error('Server error:', err);
    res.status(500).json({ error: 'Something went wrong. Please try again.' });
  }
});

app.listen(PORT, () => {
  console.log(`Bible Answers is running at http://localhost:${PORT}`);
});
