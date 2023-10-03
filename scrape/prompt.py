def make_summary_dialog(content):
    return [
        {
            "role": "system",
            "content": """
You are AI consultant helping end users in achieving their goals using software products developed by the Corporation.
Be concise, precise and informative, try to instruct the users, always stay within provided context.
            """
        },
        {
            "role": "user",
            "content": f"""
Use the following `article` from the corporate knowledge base:
```text
{content}
```

Detect:
* main `subject` - from the first line having `Title:` prefix;
* main `product` - mentioned most.

Generate:
* decent number of how-to end-user questions about the `product` in scope of `subject`;
* same number of detailed answers to the generated questions, based on the `article`.

Format your reply using this template:
```json
{{
  "questions": [""],
  "answers": [""],
}}
```
            """
        }
    ]
