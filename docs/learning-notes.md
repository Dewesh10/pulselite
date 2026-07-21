Learning Notes — PulseLite Internship
Week 1 — Things I Actually Learned (Not Just Used)

Kafka — The Conveyor Belt
Before this week, I had no idea what Kafka was. I had seen the name in job descriptions and thought it was some complex thing only senior engineers use.
Turns out the idea is simple. Imagine a factory conveyor belt. One worker (the producer) puts items on the belt. Another worker (the consumer) picks items off the belt. They never talk to each other directly. If the consumer takes a break, items just pile up on the belt and wait. Nothing is lost.
That's Kafka. Your producer puts messages (Reddit/HN posts) onto a "topic" (like a named belt). Your consumer reads from that topic. They're completely independent.
Why does this matter? Because if your processor crashes at 2am, you don't lose data. It waits in Kafka until the processor comes back up. That's why real companies use it.
Commands I learned:

Start Kafka: docker-compose up -d
Create a topic: docker exec pulselite-kafka-1 kafka-topics --create --topic hn-posts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
Check running containers: docker ps


Docker — Running Software Without Installing It
Before this week, installing Kafka would have meant downloading Java, configuring environment variables, setting up Zookeeper separately — probably 2 hours of pain.
Docker changes that. Think of Docker like a lunchbox. Everything the software needs — the food, the spoon, the napkin — is packed inside the box. You just open the box (run the container) and it works. You don't need to install anything on your actual computer.
docker-compose.yml is just a recipe file that tells Docker "run these two boxes — Kafka and Zookeeper — and connect them to each other."
One command (docker-compose up -d) and both are running. That's the power of Docker.
Lesson learned the hard way: always check if the Docker image you're using still exists on Docker Hub. bitnami/zookeeper was removed and we had to switch to confluentinc/cp-zookeeper. Always verify before putting an image name in your compose file.

VADER Sentiment — Understanding Emotions in Text
VADER stands for Valence Aware Dictionary and sEntiment Reasoner. Sounds complicated but the idea is dead simple.
Someone wrote a dictionary of thousands of words, each with a score. "Amazing" has a high positive score. "Terrible" has a high negative score. "The" has zero score. VADER reads your text, adds up all the word scores, and gives you one final number between -1 and +1.
-1 = very negative ("This is a disaster")

0 = neutral ("The meeting is at 3pm")

+1 = very positive ("This is absolutely amazing")
We use it on Hacker News post titles. So "AI coding will be more expensive than human developers" gets a negative score, while "DeepMind makes breakthrough in protein folding" gets a positive score.
No model training. No GPU. No dataset. Just import and use. That's why we picked it for a 2nd year internship project.

DuckDB — The Database That Lives in a File
Most databases need a server running 24/7. MySQL, PostgreSQL — you start a server, then connect to it, then query it. That's overkill for a small project.
DuckDB is different. It's just a file — pulselite.db — that sits in your project folder. You import it in Python, connect to it, and run SQL queries. No server. No setup. No configuration.
It's like Excel but queryable with SQL and 100x faster. Perfect for storing our time-series data (post volumes per minute, sentiment scores, anomaly alerts).
The one thing that bit us: DuckDB doesn't support INSERT OR REPLACE unless you have a primary key defined. We had to use DELETE then INSERT instead. Small thing but cost 10 minutes of debugging.

Git — Version Control Done Right
Git is something everyone says they know but most people only half-know. Here's what I actually learned this week by making mistakes:
The .gitignore file must be created before you install anything. I learned this the hard way when the entire venv folder (42,000 files, 425MB) got pushed to GitHub and choked the connection. The fix was git rm -r --cached venv/ followed by a force push. Not fun.
Rules I now follow:

Create .gitignore on Day 1, before pip install
Commit small and often — even a README update is worth a commit
Write meaningful commit messages — "feat: producer sends to Kafka" is better than "update"
git reset --hard origin/main is your undo button when things go wrong locally
git log --oneline shows you a clean history of all your commits


The Mistake I'm Most Glad I Made
Choosing Reddit as a data source without checking if the API was available in India. It wasn't. Reddit blocked new app registrations.
This forced me to find an alternative (Hacker News), which turned out to be better — no auth, more reliable, works everywhere. The lesson: always validate your data source before building anything around it. Five minutes of checking saves five hours of debugging.

What I'd Do Differently From Day 1

Create .gitignore before pip install — always
Check API availability and rate limits before choosing a data source
Verify Docker image names on Docker Hub before writing docker-compose.yml
Read the error message fully before panicking — most errors tell you exactly what's wrong


Week 4 — Reliability, and Learning to Trust But Verify

Kafka Auto-Commit — The Silent Data Loss Trap
Going in, I assumed Kafka just "handles" offset tracking for you automatically, and that's basically true — until you actually think about what "automatically" means. Auto-commit fires on a timer, not based on whether your code actually finished processing a message. So if the processor crashes between an auto-commit and finishing its work, that message is gone — Kafka thinks you've already handled it, even though you haven't.

The fix was switching to manual commits — only telling Kafka "I'm done with this message" after it's actually fully processed and saved. I proved this actually works by literally force-killing the processor mid-run (Stop-Process -Force, not a graceful Ctrl+C) and watching it restart and pick up exactly where it left off, with nothing lost or duplicated. That test taught me more about Kafka than any explanation could have — seeing it recover live made the concept real instead of theoretical.

Timezones — A Bug That Waited Three Weeks to Show Up
I had one timestamp using datetime.now() (my local time, no timezone label) and another using datetime.utcnow() (UTC, also no label). Both worked fine for weeks. Then I added a feature that compared the two directly, and got:
can't subtract offset-naive and offset-aware datetimes
The lesson: code that "works" isn't the same as code that's correct. A bug can sit completely silent until the exact right combination of circumstances triggers it.

Kafka's Weird Startup Failures Are Normal
More than once, Kafka refused to start after I closed a terminal the wrong way or my laptop slept mid-run, throwing a Zookeeper error about a broker ID conflict. First time, I assumed I'd broken something. Turns out this is a known, common issue — the fix is always docker-compose down -v then docker-compose up -d again. Lesson: when a tool fails in a weird way, check if it's a known quirk before assuming you broke it.

Process Matters Too, Not Just Code
I realized partway through that I hadn't been filing the weekly GitHub Issue check-ins the internship program expects — I was heads-down on the actual build and the process side slipped. Caught up by filing all four weeks honestly, clearly marked as retrospective, with real dates pulled from my actual commit history rather than pretending they were on time. Lesson: being behind on process and being honest about it is a lot better than staying quiet and hoping nobody checks.


Written by Dewesh | B.Tech CSE-AIDE | June 2026