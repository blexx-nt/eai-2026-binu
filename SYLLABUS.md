# Syllabus and assessment rules

**Enterprise Application Integration · Turība University · autumn 2026**
Mārtiņš Leitass · <martins.leitass@turiba.lv>

I answer on working days. There is no formal response time attached to that, so send anything time-critical early rather than the evening before a cut-off. A request counts only once I have acknowledged it, by email or through the portal.

This document is the contract. Where anything else in this repository, in a deck, or said out loud in a session contradicts it, this document wins.

---

## 1. The progression gate

**Every assignment must be *submitted* before its hard cut-off. Miss one and your capstone is not graded, which fails the course.**

That is eight submissions: Session 0, then PA1 through PA7. The hard cut-off is always the deadline **plus seven days**.

**Session 0 carries no weight in your grade.** It is pass/fail, and it is on this list because an environment that does not work is the most common reason a student disappears from this course. The gate is how that gets caught in week one instead of week eight. The seven that *are* weighted are PA1 through PA7 (§4).

| Assignment | Deadline, 20:00 | Hard cut-off, 20:00 |
|---|---|---|
| Session 0 - Docker pre-flight | 2026-09-14 | 2026-09-21 |
| PA1 - legacy file ingestion | 2026-09-14 | 2026-09-21 |
| PA2 - RabbitMQ publish and consume | 2026-09-28 | 2026-10-05 |
| PA3 - splitter, router, aggregator | 2026-10-05 | 2026-10-12 |
| PA4 - three sources to canonical | 2026-10-12 | 2026-10-19 |
| PA5 - events, DLQ, idempotency | 2026-10-26 | 2026-11-02 |
| PA6 - contract-first API and saga | 2026-11-02 | 2026-11-09 |
| PA7 - port the saga to Temporal | 2026-11-09 | 2026-11-16 |
| **PA8 - the capstone** | **2026-11-25** | - |

All dates 2026, all times **Europe/Riga**.

"Submitted" means submitted, not finished. A cut-off is cleared by an entry in the portal pointing at a real commit - nothing about quality.

### The part people get wrong

The late penalty (§2) reaches zero marks at 50 hours. The cut-off is at 168 hours. **Between those two points a submission earns no marks but keeps your
capstone alive.**

So if you are four days late on PA3 and it is worth nothing, submit it anyway. It costs you an evening and it is the difference between losing about 7% of your final grade and losing the course.  This is the single most important sentence in this document.

---

## 2. Lateness

**−2% of that assignment's mark per *started* hour after the deadline, floored at zero.**

One second past 20:00 has started the first hour. The form computes it; there is nothing to negotiate.

| Late by | Penalty | A perfect submission scores |
|---|---|---|
| on time | 0% | 100% |
| 1 minute – 1 hour | −2% | 98% |
| 12 hours | −24% | 76% |
| 24 hours | −48% | 52% |
| **25 hours** | **−50%** | **50%** - exactly the pass threshold |
| 26 hours | −52% | 48% - below it |
| 50 hours or more | −100% | 0% |
| after the cut-off (168 h) | not accepted | the capstone is not graded |

The penalty applies to the assignment's mark, not to your final grade directly. An assignment scoring 0 still counts as one of the seven, so a zero costs you one seventh of the homework half - about 7.1 percentage points of your final grade.

---

## 3. How each mark is composed

| | Share | What it is |
|---|---|---|
| Automated tests | **70%** | Public tests plus hidden tests, run against your submitted commit |
| Manual | **30%** | The quality of your ADR, and the honesty of your self-assessment |

The manual 30% is not a participation mark. An ADR that restates the assignment brief scores badly; one that names a real alternative and a real cost scores well. A self-assessment claiming work you did not do scores worse than one that accurately reports a partial implementation - the tests already tell me what works, so the only information your self-assessment adds is whether your judgement can be trusted.

**Hidden tests** are never published. They test the same requirements as the public tests. They exist so that code written to satisfy the visible tests specifically does not score well.

---

## 4. The final grade

| | |
|---|---|
| Homework | **50%** - the average of your seven assignment marks, PA1 to PA7, after penalties |
| Capstone | **50%** |

Each of the seven is one seventh of the homework half - about 7.1% of your final grade. Session 0 is not among them; it is pass/fail and gated only (§1).

**Both halves must reach 50% independently.** A homework average of 80% and a capstone of 40% is a fail. So is the reverse.

The capstone is graded only if the progression gate (§1) is clear.

### From percentage to the 10-point scale

| Final % | Mark | |
|---|---|---|
| 95-100 | **10** | izcili |
| 90-94 | **9** | teicami |
| 80-89 | **8** | ļoti labi |
| 70-79 | **7** | labi |
| 60-69 | **6** | gandrīz labi |
| 50-59 | **5** | viduvēji |
| under 50 in either half | **fail** | |

Your final percentage is rounded to the nearest whole number once, at the very end, after penalties. 89.5% is a 9. 89.4% is an 8. Nothing is rounded before that point.

**This course's pass floor is 50%, which is above the national minimum.** A mark of 4 - *gandrīz viduvēji* - is therefore not reachable here. Under 50% in either half is a fail, and so is a capstone submitted with the progression gate still open.

---

## 5. The clock

**The clock is the portal's server time, in Europe/Riga.**

Not your commit timestamp. Not when you meant to submit. Not the timestamp on an email. Not your machine's clock, and not your timezone if you are submitting from elsewhere.

Every deadline in this course is at **20:00**, stated on the assignment page, in Europe/Riga time.

---

## 6. The graded commit

**The graded commit is the SHA at `HEAD` of your named branch at the moment you submit. Later pushes are not seen.**

The form resolves and stores that SHA when it accepts your submission, and that is the code I grade. If you submit and then push a fix, the fix does not exist as far as this course is concerned.

Submit when you are done, not when you start. And check what you are submitting: the form validates your repository URL and branch with a real `git ls-remote` before it accepts anything, so a private repository, a typo or a wrong branch name fails **at submission time**, while you can still fix it.

---

## 7. Resubmission

**Before the deadline:** resubmit as often as you like, free of charge. The last submission before the deadline is the one that counts.

**After the deadline:** a resubmission **restarts the penalty clock from its own timestamp**. If you submitted 3 hours late (−6%) and resubmit at 30 hours late, your penalty is −60%, not −6%. Resubmitting late is almost always the wrong move - improving a −6% submission to a −60% one is not an improvement.

---

## 8. Extensions

Requested **through the portal**, before the deadline wherever the circumstance allows it.

**Qualifying:**

- Documented illness
- Erasmus arrival or departure dates
- Military service
- Other circumstances the university formally recognises

**Not qualifying:**

- Workload, in this course or generally
- Deadlines in other courses
- Forgetting, or misreading the deadline
- **Laptop problems. Docker problems.** Session 0 exists specifically to remove these as a reason, two weeks before they can cost you anything

An extension moves your personal deadline for that assignment, and the cut-off moves with it. It is recorded in the form; a verbal agreement in a corridor is not an extension.

Any extension must be approved by lecturer in written form (mostly via email). Required documents for extension will be discussed once that will be required and as per university guidelines.

---

## 9. Late enrolment

If you join the course after it has started, every assignment already published becomes due **14 days from your enrolment date**, and its cut-off 7 days after that. These are stored as your personal deadlines in the form; you will see them on your standing page.

Assignments not yet published follow the normal calendar.

---

## 10. The exam

One day, at the end of the semester, approximately **20 minutes per student**.
Actual date will be published as soon as known. Goal is to have not earlier than 2 weeks after last regular planned lecture.

It is a defence of your capstone, and it has two parts:

1. **You explain your own system.** Its architecture, why you made the decisions in your ADRs, where it would break first.
2. **An incident drill.** I hand you a copy of *your own* capstone with a fault introduced into it, and you diagnose it using the logs, traces and tests you built.

The drill is the reason the capstone integrates your own PA4–PA7 work rather than starting fresh. Someone who built the system can find a planted fault in about ten minutes. Someone who did not, cannot - and that is the point.

---

## 11. Sessions and attendance

> **Provisional — this section may change.** I am confirming the university's current attendance regulation. If it requires something different I will update this section and announce the change in BATIS. What will not change is this: attendance is never a component of your mark in this course, and nothing in §1 to §10 has an attendance clause.

**You are expected to attend, and attendance is registered by scanning a QR code shown in the session.**

**Staying is your own decision.** Scan the code, and if you then judge that the morning is better spent elsewhere, leaving is your right and I will not think less of you for it. I would rather you spent those hours on something you actually chose than sit through 195 minutes you had already decided not to use. The one thing that serves nobody is a room of people who are present and asleep.

### What attendance does not do

- It earns you no marks. There is no participation component
- It buys you no extension and no deadline exception
- It is not a prerequisite for anything in §1 to §10

Nothing bends for a student who attended every session, and nothing hardens for one who attended none.

### The content does not depend on it either

Everything you need in order to pass is published in this repository, ahead of time: the objectives seven days before each session, the decks as pre-reading, the assignment briefs and their public tests complete on publication. That is deliberate and it obliges me, not you. If reading this repository is all you ever do, you can pass this course.

### So what is a session for?

| | |
|---|---|
| 20 min | Overview — the session's learning outcomes and the parts people actually get stuck on |
| 40 min | **Q&A — your questions**, prepared in advance |
| 15 min | Break |
| 90 min | **Open lab** — you work on the assignment, I circulate |
| 30 min | Assignment briefing and wrap |

195 minutes of direct access to someone who has built and operated these systems in production. That is the thing a document cannot give you, and it is the only reason to stay in the room once you have scanned.

**Which means it is on you to make it worth staying.** Each `objectives/sN.md` names the hard parts of that session and gives you self-check questions; whatever you cannot answer is your question for the Q&A. Bring it written down. A Q&A where nobody has prepared anything is forty minutes of me talking, which is precisely the material you could have read.

The same applies to the lab: it is unstructured, and I circulate. Arrive with a working environment (§1, Session 0) and a specific problem, and you get an hour and a half of one-to-one help on it. Arrive with neither and you get an hour and a half of sitting in a room.

---

## 12. Academic honesty and AI

You will use AI tools in this course. I assume it and I do not object to it.
Two things are required of you:

1. **Disclose it.** The capstone requires an AI-usage disclosure. Say what you used and for what. Nobody has ever been marked down for an honest disclosure.
2. **Be able to defend every line.** The exam is oral, on your own code, with a fault planted in it. Code you did not understand when you submitted it is code you cannot defend in November.

Submitting another student's work as your own is plagiarism and is handled under university regulations. Your repositories are public and are mirrored at grading time; similarity across submissions is checked.

---

## 13. What to do when something goes wrong

| | |
|---|---|
| Stuck on an assignment | [CONTRIBUTING.md](CONTRIBUTING.md). Thirty minutes, then ask |
| Environment broken | `s0/README.md` troubleshooting table, then ask |
| Will miss a deadline | Request an extension through the form, **before** the deadline |
| Missed a deadline | Submit anyway, immediately. See §1, "the part people get wrong" |
| Disagree with a mark | Raise it with me **within 7 days** of the mark being published. Bring the specific evidence and your reasoning, not just the disagreement, and we will go through it |

The failure mode this course is built to prevent is a student who hits a problem in week two, says nothing, and is mathematically eliminated by week eight without ever having been asked a question about integration. If you are heading that way, the cost of telling me is zero.
