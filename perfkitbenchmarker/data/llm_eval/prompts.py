"""Loads and categorizes prompts from the prompts.txt file.

This module reads the prompts.txt file, splits it into individual prompts,
and categorizes them into summarization and open-ended prompts based on
the presence of the word "summarize".
"""

SUMMARIZATION_PROMPTS = {}
OPEN_ENDED_PROMPTS = {}

with open('prompts.txt', 'r', encoding='utf-8') as f:
  prompts = f.read().split('---')
  for i, prompt in enumerate(prompts):
    if 'summarize' in prompt.lower():
      # Set to 4096, the maximum supported by some models
      # (e.g., gpt-4-turbo).
      SUMMARIZATION_PROMPTS[f'prompt_{i+1}'] = {
          'text': prompt.strip(),
          'max_tokens': 4096,
      }
    else:
      OPEN_ENDED_PROMPTS[f'prompt_{i+1}'] = {
          'text': prompt.strip(),
          'max_tokens': None,
      }
