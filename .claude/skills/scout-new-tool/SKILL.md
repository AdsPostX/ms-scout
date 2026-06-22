# Add a New Claude Tool to Scout

**Triggers:** "add a new tool", "new Claude tool", `/scout-new-tool`

## Workflow

1. **Read `scout_agent.py` SYSTEM_PROMPT + TOOLS** — understand naming conventions and how intent routing lines are written before touching anything.

2. **Define the output shape first.** Name the column names, types, and row grain before writing any code. If you can't describe the shape from memory, run the raw query in ClickHouse first.

3. **Add the tool definition** to `scout_tools_definitions.py` (name, description, input_schema).

4. **Implement the handler** in the appropriate `scout_tools_*.py` file:
   - Revenue/CVR → `scout_tools_revenue.py`
   - Campaign analytics → `scout_tools_campaigns.py`
   - Publisher health → `scout_tools_publisher.py`
   - Offer/supply gap → `scout_tools_offers.py`
   - Admin/status → `scout_tools_admin.py`

5. **Add the SQL** as a named function in the appropriate `queries_*.py` file. Never put SQL inline in the handler.

6. **Add an entry to `scout_agent.py:TOOL_MAP`** — `"tool_name": handler_fn`.

7. **Add a numbered intent routing line** to SYSTEM_PROMPT in `scout_agent.py`. Missing line = tool never gets called.

8. **Add a smoke test** to `smoke_test.py`.

9. **Run `python3 smoke_test.py`** — must pass before commit.

## Anti-patterns
- SQL inline in TOOL_MAP handlers
- Handler with no smoke test coverage
- Tool with no intent routing line in SYSTEM_PROMPT
- Handler calling ClickHouse directly — always route through `queries_*.py`
