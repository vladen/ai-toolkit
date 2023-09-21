def make_context_prompt(content, summary, url):
  return f"""
Some information related to this subject was found on an official we page.
Use it in your answer.

Page URL: {url}
Page summary:
```text
{summary}
```
Page content:
```json
{content}
```
  """

def make_summary_prompt(content):
  return f"""
Use this text as `context` to generate your reply for imaginary end-user
as you were an assistant chat on a corporate web page:
```text
{content}
```
Generate 2 entities for the `context`:
1. one summary - several sentences, up to 50% of the `context`, all important subjects preserved;
2. three concise and simple end-user questions - relevant to the `context`.
Format your reply as `json` object with 2 properties: `questions` and `summary`.
"""
