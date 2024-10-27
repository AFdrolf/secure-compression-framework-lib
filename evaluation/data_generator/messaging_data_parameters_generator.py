import random


LLM_PROMPT = ""
LONG_TAIL_MODEL_CONSTANTS = {
    "high_activity_chats_percentage": 0.10,    # Percentage of chats that have high activity
    "high_activity_messages_percentage": 0.80,    # Percentage of total messages sent by the high-activity chats
    "high_activity_chat_min_messages": 0.5    # To compute the minimum number of messages to be considered a high-activity chat
}

def generate_chats_LLM_prompt(number_chats, number_messages, communication_model):
    """Given some seed parameters, generates the prompt to feed to an LLM for generating a transcript of chats.
    
    This function corresponds to a single iteration of the evaluation.

    Args:
    -----
    number_chats: Number of chats
    number_messages: Total number of messages sent across all chats
    """
    number_messages_per_conversation = []

    # All chats have the same number of messages
    if communication_model == "even":
        for i in range(number_chats):
            r = number_messages//number_chats
            number_messages_per_conversation.append(r+1 if i < number_messages % number_chats else r)

    # All chats have a random number of messages
    elif communication_model == "random":
        number_messages_per_conversation = [1] * number_chats
        _allocate_messages_randomly_to_chats(number_messages_per_conversation, number_messages-number_chats)

    # Most chats have a few messages, while a few chats have many messages
    elif communication_model == "long_tail":
        high_activity_chats_total = number_chats*LONG_TAIL_MODEL_CONSTANTS["high_activity_chats_percentage"]
        high_activity_messages_total = number_messages*LONG_TAIL_MODEL_CONSTANTS["high_activity_messages_percentage"]
        high_activity_chats_avg_messages = high_activity_messages_total//high_activity_chats_total
        high_activity_chats_min_messages = high_activity_chats_avg_messages*LONG_TAIL_MODEL_CONSTANTS["high_activity_chat_min_messages"]

        # First, allocate messages to the high-activity chats
        messages_allocated = 0
        for _ in range(high_activity_chats_total):
            number_messages = random.randint(high_activity_chats_min_messages, high_activity_chats_avg_messages)
            number_messages_per_conversation.append(number_messages)
            messages_allocated += number_messages
        # If there are still messages for the high activity chats left, allocate them randomly to these
        _allocate_messages_randomly_to_chats(number_messages_per_conversation, high_activity_messages_total-messages_allocated)

        # Then, allocate remaining messages to the rest of the chats
        low_activity_chats = [1] * (number_chats - high_activity_chats_total)
        number_messages_per_conversation += _allocate_messages_randomly_to_chats(low_activity_chats, number_messages-high_activity_messages_total, high_activity_chats_min_messages-1)


def _allocate_messages_randomly_to_chats(chats_list, number_messages, max_messages_chat=None):
    for _ in range(number_messages):
        chat_ix = random.randint(0, len(chats_list)-1)
        while max_messages_chat and max_messages_chat <= chats_list[chat_ix]: 
            chat_ix = random.randint(0, len(chats_list)-1)
        chats_list[chat_ix] += 1
    return chats_list