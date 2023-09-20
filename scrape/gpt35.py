import json
import openai
import os

prompt = """
Perform 2 tasks with the content scraped from "https://adobe.com/" web page and formatted as CSV:
1. Summarise it in several sentences (max 25% of the original text), preserve all important subjects;
2. Generate 3 relevant end-user questions that would be perfectly answered on that page.

Format your reply using this template:
```
Summary:
  sentence
  sentence
  sentence
  ...

Questions:
  question
  question
  question
```

---

```CSV
Type,Text
Title,Ready for standout content? Meet Adobe Express.
NarrativeText,"Make and share beautiful content with ease. Choose from thousands of professional-designed templates for fast social posts, flyers, banners, and more."
Title,Get Adobe Express free | Adobe Flash Player EOL General Information Page
NarrativeText,"Since Adobe no longer supports Flash Player after December 31, 2020 and blocked Flash content from running in Flash Player beginning January 12, 2021, Adobe strongly recommends all users immediately uninstall Flash Player to help protect their systems."
NarrativeText,Some users may continue to see reminders from Adobe to uninstall Flash Player from their system. See below for more details on how to uninstall Flash Player.
Title,"UPDATED: January 13, 2021"
NarrativeText,"Adobe stopped supporting Flash Player beginning December 31, 2020 (“EOL Date”), as previously announced in July 2017."
Title,Apple | Facebook | Google | Microsoft and | Mozilla.
NarrativeText,"After the EOL Date, Adobe will not issue Flash Player updates or security patches. Adobe strongly recommends immediately uninstalling Flash Player. To help secure your system, Adobe blocked Flash content from running in Flash Player beginning January 12, 2021. Major browser vendors have disabled and will continue to disable Flash Player from running. | Click “Uninstall” when prompted by Adobe, or follow these manual uninstall instructions for Windows and Mac users. | Apple Safari version 14, released for macOS in September 2020, no longer loads Flash Player or runs Flash content. Please visit | Apple’s Safari support for more information."
NarrativeText,"Flash Player may remain on your system unless you uninstall it. Uninstalling Flash Player will help secure your system since Adobe will not issue Flash Player updates or security patches after the EOL Date. Adobe blocked Flash content from running in Flash Player beginning January 12, 2021 and the major browser vendors have disabled and will continue to disable Flash Player from running after the EOL Date."
NarrativeText,"Since Adobe is no longer supporting Flash Player after the EOL Date, Adobe blocked Flash content from running in Flash Player beginning January 12, 2021 to help secure your system. Flash Player may remain on your system unless you uninstall it."
Title,Please visit
NarrativeText,http://www.adobe.com/products/flashplayer/tech-specs.html for the latest list of Flash-supported browsers and operating systems. | You should not use unauthorized versions of Flash Player. Unauthorized downloads are a common source of malware and viruses.
NarrativeText,"No. Adobe has removed Flash Player download pages from its site. Adobe blocked Flash content from running in Flash Player beginning January 12, 2021."
NarrativeText,Adobe will not issue Flash Player updates or security patches after the EOL Date. Adobe strongly recommends uninstalling Flash Player immediately.
Title,Discover Creative Cloud Apps | Acrobat | Acrobat Pro
NarrativeText,"Create, edit, sign, and manage your PDFs — quickly, easily, anywhere. Learn more"
Title,Start free trial | Adobe Express
NarrativeText,Quickly and easily make standout content from thousands of beautiful templates. Learn more.
Title,Start for free | Adobe Stock
NarrativeText,"Choose from millions of photos, drawings, video clips, and more to add to your creations. Learn more"
Title,Start free trial
```
"""

openai.api_key = os.getenv("OPENAI_API_KEY")
response = openai.Completion.create(
  model="gpt-3.5-turbo-instruct",
  prompt=prompt,
  max_tokens=3000,
  temperature=0.2
)

print(response.choices[0].text)
