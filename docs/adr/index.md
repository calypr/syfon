An Architecture Decision Record (ADR) is a short document that captures an important engineering decision about a system, along with its context and consequences. It creates a historical log of *why* key architectural choices were made, so future readers can understand the reasoning and avoid repeating old discussions.

## What an ADR is

An ADR typically:

* Describes a *single* significant decision (e.g. "Use Postgres instead of MySQL").
* Records the context and constraints at the time of the decision.
* Explains the chosen option and alternatives considered.
* States the consequences (both positive and negative).
* Is immutable once accepted; changes are recorded as new ADRs that supersede older ones.

They are usually stored in a dedicated directory like `docs/adr/` and named with an ordered prefix, for example:

* `adr/0001-use-rest-api.md`
* `adr/0002-choose-postgres.md`

This ordering helps track the evolution of architecture over time.

## How to read an ADR

Most ADRs follow a consistent template. When you open one (for example via your `docs/adr/index.md` list or nav):

1. **Title / ID**  
   At the top, find the ADR number and descriptive title. This tells you *what* decision is being documented.

2. **Status**  
   Look for a `Status` section, often one of:
    * `Proposed` \- under discussion.
    * `Accepted` \- this is the current decision.
    * `Superseded by ADR-XXXX` \- replaced by a newer decision.
      Focus mainly on `Accepted` ADRs; if it is superseded, go read the newer one.

3. **Context**  
   This section explains:
    * The problem being solved.
    * Relevant constraints (deadlines, existing systems, team skills).
    * Requirements or forces that influenced the decision.
      Read this to understand *why* there was a decision to make at all.

4. **Decision**  
   A concise statement of what was chosen. For example:
   *“We will use PostgreSQL as the primary relational database for all services.”*  
   This is the core answer you are looking for.

5. **Consequences**  
   Details the impact of the decision:
    * Benefits: what becomes easier or better.
    * Drawbacks: trade\-offs, new limitations, or risks.
    * Follow\-up tasks or required changes.
      This helps you understand how the decision affects current and future work.

6. **Alternatives / Rationale (if present)**  
   Some ADRs include:
    * Options considered and why they were rejected.
    * Links to discussions, tickets, or design docs.
      This is useful when you wonder *“Why didn’t we just do X?”*.

## How to use ADRs in practice

* **Before changing architecture**:  
  Search the `adr/` directory for relevant ADRs and read:
    * The latest `Accepted` ADR on that topic.
    * Any ADRs that *supersede* older ones.

* **When confused about a pattern or technology choice**:  
  Find the ADR whose title matches that area (e.g. logging, databases, APIs) to see the original reasoning.

* **When proposing a change**:  
  If you need to overturn an existing decision, write a new ADR that:
    * References the old one.
    * Marks the old one as `Superseded`.
    * Explains new context or requirements.

By consistently reading ADRs this way, you get both the *current architectural rules* and the *history* that explains how the system ended up in its present shape.