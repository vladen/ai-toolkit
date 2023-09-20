def make_context_prompt(document, summary, url):
  return f"""
Some information relevant to the subject was found in the Internet and provided below.
Use this information in your answer.

Chat GPT response:
```text
{summary}
```

Page content:
```csv
{document}
```

Page URL: {url}
  """

def make_summary_prompt(document, url):
  return f"""
Perform 2 tasks with the content scraped from "{url}" web page:
1. Summarise it in several sentences, make summary size approximately 50% of text size in chars, keep all important subjects;
2. Generate 3 relevant end-user questions that would be perfectly answered on that page.

Format your reply using this template:
```text
Summary:
  text
Questions:
  list
```

Scraped data:
```csv
{document}
```
"""
