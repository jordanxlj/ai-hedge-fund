# Investment Agent Prompts

This directory contains centralized prompt templates for all investment agents in the AI hedge fund system.

## Overview

Previously, prompt templates were embedded directly within each agent's code. This refactoring extracts all prompts into separate, reusable modules for better maintainability and consistency.

## Structure

Each agent has its own prompt file with the following pattern:

```python
from langchain_core.prompts import ChatPromptTemplate

AGENT_NAME_SYSTEM_PROMPT = """..."""
AGENT_NAME_HUMAN_PROMPT = """..."""

def get_agent_name_prompt_template():
    return ChatPromptTemplate.from_messages([
        ("system", AGENT_NAME_SYSTEM_PROMPT),
        ("human", AGENT_NAME_HUMAN_PROMPT),
    ])
```

## Available Agents

### Completed Extractions
- ✅ **Warren Buffett** (`warren_buffett.py`) - Value investing with circle of competence
- ✅ **Bill Ackman** (`bill_ackman.py`) - Activist value investing with brand focus
- ✅ **Ben Graham** (`ben_graham.py`) - Classic value investing with margin of safety
- ✅ **Peter Lynch** (`peter_lynch.py`) - Growth at reasonable price (GARP)
- ✅ **Charlie Munger** (`charlie_munger.py`) - Quality businesses with mental models
- ✅ **Cathie Wood** (`cathie_wood.py`) - Disruptive innovation and exponential growth
- ✅ **Stanley Druckenmiller** (`stanley_druckenmiller.py`) - Asymmetric risk-reward with momentum
- ✅ **Aswath Damodaran** (`aswath_damodaran.py`) - Rigorous DCF valuation analysis
- ✅ **Michael Burry** (`michael_burry.py`) - Contrarian deep value investing
- ✅ **Phil Fisher** (`phil_fisher.py`) - Growth investing with scuttlebutt research
- ✅ **Rakesh Jhunjhunwala** (`rakesh_jhunjhunwala.py`) - Growth at reasonable price with quality focus

### Pending Extractions
- ⏳ **Portfolio Manager** - Portfolio construction and risk management
- ⏳ **Risk Manager** - Risk assessment and position sizing

## Usage

Import and use the prompt templates in agent files:

```python
from src.prompts.warren_buffett import get_warren_buffett_prompt_template

def generate_buffett_output(ticker, analysis_data, state):
    template = get_warren_buffett_prompt_template()
    prompt = template.invoke({
        "analysis_data": json.dumps(analysis_data, indent=2), 
        "ticker": ticker
    })
    # ... rest of the function
```

## Benefits

1. **Centralized Management**: All prompts in one location for easy updates
2. **Consistency**: Standardized format across all agents
3. **Reusability**: Prompts can be shared or adapted for new agents
4. **Version Control**: Better tracking of prompt changes over time
5. **Testing**: Easier to unit test prompt templates separately
6. **Maintenance**: Simpler to update investment philosophies or output formats

## Investment Philosophies

Each agent's prompt captures their unique investment philosophy:

- **Warren Buffett**: Circle of competence, economic moats, long-term value
- **Bill Ackman**: Brand strength, activism potential, concentrated positions
- **Ben Graham**: Net-net analysis, Graham Number, conservative metrics
- **Peter Lynch**: PEG ratio, ten-bagger potential, understandable businesses
- **Charlie Munger**: Mental models, predictability, quality over price
- **Cathie Wood**: Disruptive innovation, exponential growth, large TAM
- **Stanley Druckenmiller**: Asymmetric opportunities, momentum, macro awareness
- **Aswath Damodaran**: DCF rigor, cost of capital, intrinsic value focus
- **Michael Burry**: Contrarian deep value, FCF yield, balance sheet strength
- **Phil Fisher**: Growth quality, management assessment, competitive advantages
- **Rakesh Jhunjhunwala**: Quality growth, margin of safety, long-term horizon

## Future Enhancements

- Add prompt versioning system
- Create prompt A/B testing framework
- Implement dynamic prompt adjustment based on market conditions
- Add multi-language support for international markets 