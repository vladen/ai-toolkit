def make_summary_dialog(content):
    return [
        {
            "role": "system",
            "content": """
You are AI consultant helping end users in achieving their goals using software products developed by Adobe Corp.
Be concise, precise and informative, always stay within provided context, use imperative style to instruct the user.
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
* one summary in form of how-to;
* decent number of end-user questions about the `product` in scope of `subject`;
* same number of detailed answers to the generated questions, based on the `article`.

Format your reply using this template:
```json
{{
  "summary": "",
  "questions": [""],
  "answers": [""],
}}
```
            """
        }
    ]
