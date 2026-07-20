# PulseLite — Reflection

## Section 1: What I Built

PulseLite watches Hacker News in real time and tells you what's actually
happening there right now — not just the posts, but the mood behind them,
whether something is suddenly getting way more attention than usual, and
whether people have quietly moved on to a different topic. It's for
anyone who wants a live feel of a community instead of just scrolling a
list of links.

The extra piece I added on top of the basic requirements was an anomaly
alert system. When post activity suddenly spikes far above normal, the
app doesn't just quietly write that down somewhere — it sends a message
straight to Discord so I'd actually notice. I added this because catching
something interesting and then never actually seeing it happen felt
pointless. I wanted it to feel alive, not just accurate.

## Section 2: What I Learned About the Tools

**Kafka** was less scary than I thought it would be, conceptually. Once I
understood that it's basically a very reliable pipe that carries messages
from one part of a system to another, most of it clicked fast. What
actually gave me trouble was keeping it running smoothly day to day. If I
closed a terminal the wrong way, or my laptop went to sleep while
everything was running, Kafka would refuse to start the next time, with a
confusing error message. It took me a while to realize this happens to
basically everyone, and the fix is simple — clear it out and restart
fresh. If a friend was starting with Kafka, I'd tell them: don't panic
when it breaks in weird ways, that's just part of using it, not a sign
you did something wrong.

**Avro and Schema Registry** sounded intimidating before I actually used
them, but the idea turned out to be simple: they make sure that if you
change the shape of your data later, old and new versions can still work
together, and if a change would actually break something, it gets caught
immediately instead of causing a mystery crash weeks later. Being able to
prove this — showing that adding a new field is safe, but removing one
isn't — made it feel real instead of theoretical.

**Streamlit and Flask** taught me a lot just by comparison. Streamlit
lets you build something that looks good very fast, but you're playing by
its rules. When I rebuilt the same dashboard in Flask, I had to handle
things myself that Streamlit had been quietly doing behind the scenes.
That was frustrating in the moment, but it made me actually understand
what a dashboard framework is doing for you, instead of just trusting it
blindly.

I also picked up a few smaller tools along the way that turned out to
matter more than I expected. Pytest was straightforward once I got past
one dumb mistake — running it without my virtual environment activated
and getting a "module not found" error that made no sense until I
realized the tool was installed somewhere my terminal wasn't looking.
Pre-commit hooks were a similar story: the tool itself was simple, but
the first run reformatted twelve files at once and flagged real issues I
hadn't noticed, like a variable I was setting but never using. It was a
small reminder that a linter isn't just nagging about style — it catches
things that are genuinely worth fixing.

One smaller but very real lesson: **timezones are sneaky**. I had one
part of my code saving time in my local timezone and another part saving
it in UTC, and neither one said which. It worked fine for weeks until I
added a feature that compared the two, and the whole app crashed. It was
a small mistake, but it taught me that "it works right now" doesn't mean
it's actually correct — some bugs just wait quietly until the right
moment to show up.

## Section 3: What I Learned About Myself

The parts of this project that don't show up on screen — making sure
data doesn't get lost or duplicated, making sure the app survives a
crash — took way more effort than I expected, and honestly, they're kind
of invisible to anyone just looking at the demo. But actually testing
it — killing the program on purpose in the middle of running it, and
watching it come back and pick up exactly where it left off with nothing
lost — felt like a real "it actually works" moment, way more satisfying
than adding a new chart.

I noticed I actually enjoy fixing broken things more than I enjoy
building new features from scratch. Tracking down why something crashed,
or why a number looked wrong, held my attention way more than designing
a new UI element did. I didn't expect that about myself going in — I
always assumed I'd enjoy building the visible, flashy parts most, but it
turned out the opposite was true. There's something satisfying about a
mystery having an actual, findable cause, versus a feature just being a
matter of taste and design choices.

I'll be honest — I also put off some of the less exciting tasks longer
than I should have. Things like cleaning up code style or auditing for
sloppy error handling kept getting pushed down my list because they don't
show up in a demo, even though I know they matter. I think that says
something about how I naturally prioritize — I lean toward whatever
produces something visible fastest, and I have to consciously push myself
to spend time on the parts that only matter if something goes wrong
later. That's a habit I want to be more aware of going forward, especially
since the internship itself pointed out that this kind of "invisible"
work is exactly what separates a working demo from something you could
actually trust.

Work-wise, I stuck to the plan more consistently than I expected to,
mostly because breaking the whole thing into small, one-step-at-a-time
pieces made it much easier to actually sit down and start, instead of
staring at a huge task and putting it off. Doing things one command at a
time, checking the result, and only then moving to the next step, kept
me from getting overwhelmed the way I usually do when a task feels big
and vague.

## Section 4: What I'd Do Differently

If I started over, I'd build the safety-net stuff — handling crashes,
avoiding duplicate data, shutting down cleanly — right from the
beginning, instead of adding it in later. It's a lot easier to build
something safe from day one than to go back and make an already-running
system safe after the fact.

I also wish someone had told me early on that Kafka acting weird after a
messy shutdown is completely normal. I spent time thinking I'd broken
something important, when really it's just a known quirk almost everyone
runs into. A little more patience with tools that misbehave, instead of
assuming I'm the one at fault, would have saved me some frustration.

## Section 5: What's Next — The 3rd Year Plan

What I have right now is a real, working version — but it's still the
early version. Going into 3rd year, I want to make it properly
bulletproof: making sure no message ever gets processed twice even if
something crashes at the worst possible moment, and handling data that
shows up late or out of order instead of assuming everything arrives
neatly in sequence. I also want to eventually rebuild part of the system
using a tool called Flink, which is built specifically for this kind of
always-running data processing, instead of the simpler approach I used
here.

Beyond the technical upgrades, I want this project to be the thing I keep
coming back to rather than a one-off assignment I finish and forget. The
foundation — the pipeline, the reliability layer, the tests — is
something I can genuinely keep building on, instead of starting over from
scratch next year. That, more than any single feature, is what I think
this internship was actually trying to teach: build one real thing well
enough that it's worth extending, instead of building three shallow
things and abandoning all of them. The detailed version of this plan is
in my 3rd Year Roadmap — this reflection is really just explaining the
"why" behind it.