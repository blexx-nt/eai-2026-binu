# Getting help

This is a course about systems that fail in confusing ways. Getting stuck is
the normal condition, not a sign that something has gone wrong with you. What
matters is how long you stay stuck silently.

---

## The thirty-minute rule

**If you have made no progress in thirty minutes, ask.**

Not two hours. Not "after I try one more thing". Thirty minutes of genuinely
no progress — not thirty minutes of productive struggle, which is different
and which you should keep doing.

Nobody in this course has ever lost a mark for asking a question. The
assignments are marked on what you submit, not on how much you suffered to get
there.

---

## Where to ask

| | |
|---|---|
| **The session Q&A** | 40 minutes of every session, on questions you bring. The highest-bandwidth help available in this course |
| **The session lab** | 90 minutes, same day. I circulate; you get one-to-one time on whatever you are actually stuck on |
| **Email** | <martins.leitass@turiba.lv> · **first choice between sessions**, for technical questions and personal ones alike |
| **Office hours** | As published in the timetable. If that does not work, ask and we can arrange an individual slot when I am free |
| **BATIS** | The official Turība system. Slide decks and course materials are published there, not in this repository |

Attendance is registered by QR scan, but staying is your own decision
([SYLLABUS.md](SYLLABUS.md) §11). The Q&A and the lab are the two places where
you get my reasoning rather than just my answer, and they are free — which
makes them the best argument for staying. If you are going to, come with
something written down; `objectives/sN.md` tells you what the hard parts are so
you know what to prepare.

Between sessions there is no chat channel for this course, so email is the route for everything: a failing test, an illness, a deadline you can see coming. Two consequences are worth knowing. Ask early rather than the night before a cut-off, because I answer on working days and no response time is promised. And ask well: a question that arrives with the four things below usually takes one reply, where a question without them takes three, and those three can span a day.

**A request counts only once I acknowledge it**, by email or through the portal. An email you sent is not an agreed extension until you have my answer in writing. If two working days pass in silence, send it again — things do get lost, and chasing me is not rude.

Email is **never** a submission channel — see [SYLLABUS.md](SYLLABUS.md) §5.

---

## How to ask so that you get an answer in one round trip

Include four things:

1. **What you were trying to do.** One sentence.
2. **What you ran.** The actual command, copied, not described.
3. **What happened.** The actual output, copied as text, not a photo of your
   screen and not a summary. If it is long, the last 30 lines.
4. **What you have already tried.**

That is the whole protocol. With those four things I can usually answer in one
message. Without them, we spend three messages establishing them first, and
those three messages might span a day.

### Copy text, not pictures

```
❌  a photo of your laptop screen
❌  "it says something about a connection error"
❌  a cropped screenshot of one line
✅  a fenced code block with the command and its output
```

Screenshots are fine for something genuinely visual — a browser rendering, the
RabbitMQ management UI, a Temporal workflow view. For terminal output, paste
the text. I cannot grep a photograph.

### A good question

> Trying to get PA2's consumer to acknowledge messages manually.
>
> ```
> $ docker compose logs consumer --tail 5
> consumer-1  | Error: Channel closed by server: 406 (PRECONDITION-FAILED)
> consumer-1  | with message "unknown delivery tag 1"
> ```
>
> I call `channel.ack(msg)` at the end of my handler. I read the amqplib docs
> on delivery tags and checked that I am not acking twice. I suspect it is
> because I set `noAck: true` when I subscribed, but I am not sure why that
> would produce this error rather than being ignored.

That gets a precise answer immediately. Note the last sentence especially: a
wrong hypothesis, stated, is far more useful to me than no hypothesis.

---

## Before you ask

Two minutes, in this order:

1. **Read the error.** Actually read it. This course's tooling — including the
   `s0` doctor — tries hard to tell you what is wrong and what to do about it.
2. **Check the assignment README.** Every brief has the failure modes I expect
   you to hit.
3. **Check `s0/README.md`** if it smells environmental: ports, containers,
   Docker not starting.

Then ask. Do not spend an evening on step 1 through 3.

---

## What I will and will not do

**I will:** read your error, tell you what class of problem it is, point you at
the concept or the line, look at your repository, and tell you when you are
solving the wrong problem.

**I will not:** write your assignment, debug your code line by line while you
watch, or tell you whether an unfinished submission "would pass" — run the
public tests, they are the same tests I run.

**Python.** PA3 is scaffolded in Python. Everywhere else you may implement in
Python instead of TypeScript and the black-box tests will grade you
identically — but no Python scaffold is provided, and Python-specific problems
are outside office hours. That is the trade you are making if you take it.

---

## Something wrong with an assignment?

Ambiguous brief, a public test that seems wrong, a link that does not resolve,
a scaffold that does not build from a clean clone — **email me immediately**. That is not a complaint, it is a bug report, and I would much
rather hear it at 22:00 the day it is published than discover it while
grading.

If an assignment is corrected after publication, the change is recorded under
a `## Changes` heading in that assignment's README with a date, and announced
in BATIS. Corrections never make a published assignment harder.

---

## If you are falling behind

Say so. Early.

The progression gate means one missed cut-off costs you the whole course, so
the situation this course is worst at handling is silence. There are real
mechanisms for real circumstances — extensions, per-student deadlines, the
catch-up window for late enrolment — and all of them work in advance and none
of them work retroactively.

And if you are past a deadline with nothing submitted: **submit what you
have, now.** A submission worth zero marks still clears the gate. See
[SYLLABUS.md](SYLLABUS.md) §1.
