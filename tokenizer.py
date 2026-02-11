import sys

def tokenize(text):
    # tokens
    token_list = []
    buffer = ""
    segment_size = 1024
    text_len = len(text)
    pos = 0

    while pos < text_len:
        segment = text[pos:pos+segment_size]
        pos += segment_size

        buffer += segment
        
        # replace all non-alphanumeric characters
        buffer_filtered = ''.join(c if (c.isalnum() or c == "'") else ' ' for c in buffer)

        # find last partial word
        last_delim = buffer_filtered.rfind(' ')
        if last_delim != -1:
            full_tokens_buff = buffer_filtered[:last_delim+1]
            buffer = buffer_filtered[last_delim+1:]

        else:
            # Entire buffer one character sequence
            # Assume one token, send to process and reset buffer
            full_tokens_buff = buffer_filtered
            buffer = ""

        segment_tokens = full_tokens_buff.lower().split()
        token_list.extend(segment_tokens)

    # process end of buffer
    if buffer:
        buffer_filtered = ''.join(c if (c.isalnum() or c == "'") else ' ' for c in buffer)
        segment_tokens = buffer_filtered.lower().split()
        token_list.extend(segment_tokens)

    # filtered_tokens = []
    # for token in token_list:
    #     if len(token) == 1 and token not in {"a", "i"}:
    #         continue
    #     if token.isdigit():
    #         continue
    #     filtered_tokens.append(token)  

    # Return token list
    return token_list
    # return filtered_tokens


def computeWordFrequencies(token_list):
    # Iterate through and count, sorting into map
    freq_map: map[str, int] = {}
    for token in token_list:
        if token in freq_map:
            # add to count
            freq_map[token] += 1
        else:
            freq_map[token] = 1
    return freq_map

def tokenize_visible_text(text):
    token_list = tokenize(text)
    frequency_map = computeWordFrequencies(token_list)
    return frequency_map