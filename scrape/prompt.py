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
Use this article from the corporate knowledge base as context.
```text
{content}
```
            """
        },
        {
            "role": "user",
            "content": """
Generate:
* decent number of how-to end-user questions about the product and its use-case described in the article;
* same number of detailed answers to the generated questions, based on the article.
Format result as json based on template:
```json
{
    "questions": [],
    "answers": [],
}
"""
        },
    ]
