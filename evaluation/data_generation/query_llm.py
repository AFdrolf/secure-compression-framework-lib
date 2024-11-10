from pathlib import Path

from openai import OpenAI

if __name__ == "__main__":
    client = OpenAI()

    prompt = """
    Generate x realistic conversations of 10 messages between users of a messaging application. 
    Each conversation should have a different subject. 
    Use a range of formal and informal writing (where informal means including texting abbreviations and not using capitalization/punctuation) and different message lengths across conversations to simulate different relationships between users.
    Please format the output for each conversation as message1 \message2 \... and output each conversation on a new line. Do not output anything else.
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": "You are a helpful assistant."}, {"role": "user", "content": prompt}],
    )

    conversations_path = Path(__file__).parent.parent / "helper_data" / "conversations.txt"
    with conversations_path.open("a") as f:
        for conversation in response.choices[0].message.content.split("\n\n"):
            parsed = conversation.split(" \\\n")
            f.write("\\".join(parsed) + "\n")
