"""
Prompt templates for all investment agents.
This module centralizes all ChatPromptTemplate definitions for easier maintenance.
"""

from .warren_buffett import get_warren_buffett_prompt_template
from .bill_ackman import get_bill_ackman_prompt_template
from .ben_graham import get_ben_graham_prompt_template
from .peter_lynch import get_peter_lynch_prompt_template
from .charlie_munger import get_charlie_munger_prompt_template
from .cathie_wood import get_cathie_wood_prompt_template
from .stanley_druckenmiller import get_stanley_druckenmiller_prompt_template
from .aswath_damodaran import get_aswath_damodaran_prompt_template
from .michael_burry import get_agent_prompt_template as get_michael_burry_prompt_template
from .phil_fisher import get_agent_prompt_template as get_phil_fisher_prompt_template
from .rakesh_jhunjhunwala import get_agent_prompt_template as get_rakesh_jhunjhunwala_prompt_template

__all__ = [
    'get_warren_buffett_prompt_template',
    'get_bill_ackman_prompt_template',
    'get_ben_graham_prompt_template',
    'get_peter_lynch_prompt_template',
    'get_charlie_munger_prompt_template',
    'get_cathie_wood_prompt_template',
    'get_stanley_druckenmiller_prompt_template',
    'get_aswath_damodaran_prompt_template',
    'get_michael_burry_prompt_template',
    'get_phil_fisher_prompt_template',
    'get_rakesh_jhunjhunwala_prompt_template',
] 