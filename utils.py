# CONSTANTS
BATCH_SIZE = 10000
TIME_LIMIT_TWO_MINUTES_MS = 120000
TIME_LIMIT_TWO_MINUTES_SEC = 120


# this is referred to when this function was created:
# https://www.geeksforgeeks.org/python-program-to-read-character-by-character-from-a-file/
import json
def read_documents(file_opened):
    '''
    Helper function: reads in documents from the opened file.
    This function accounts for even the most extreme case (the data file stored in one line).
    The argument is an opened file, it should not be the directory. 
    The return value is either None (only if the document is malformated) or a dictionary(json section).
    '''
    indentation_level = -1
    last_char_space = False
    # important: do NOT let brackets in string mislead the algorithm!
    not_in_string = True
    # important: do NOT let escaped quotation mark \" mislead the algorithm!
    escaped = False

    read_characters = []
    while True:
        curr_char = file_opened.read(1)
        # technically speaking, this should never not be reached if the json file is in proper format.
        if len(curr_char) == 0:
            print("ERROR: the json file provided to read_documents is very likely malformed. Watch for curly brackets pairing.")
            yield None
            break
        else:
            # escaped character are left as-is
            if (escaped):
                read_characters.append(curr_char)
                escaped = False
            # if not escaped, handle more complex behaviour
            else:
                # this variable denotes whether the current character shall be included as part of document.
                should_append = True

                # in-string flag toggling; contents in string are left as-is
                if (curr_char == '\"'):
                    not_in_string = not not_in_string
                # escaped flag toggling
                elif (curr_char == '\\'):
                    escaped = True
                
                # space-like characters
                if (not_in_string and curr_char in " \t\n"):
                    # convert several consecutive space-like character into one
                    should_append = not last_char_space
                    curr_char = ' '
                    last_char_space = True

                # OPENING brackets handling BEFORE the character is recorded
                if (not_in_string and curr_char in "{["):
                    indentation_level += 1
                    # clear the characters when starting to read a new document
                    if (indentation_level == 1):
                        read_characters = []
                # record character if needed
                if (should_append):
                    read_characters.append(curr_char)
                # CLOSING brackets handling AFTER the character is recorded
                if (not_in_string and curr_char in "]}"):
                    indentation_level -= 1
                    # exit loop on the last layer 
                    if (indentation_level < 0):
                        break
                    # if a document is concluded, yield it. The read_characters are cleared when the opening bracket is read.
                    if (indentation_level == 0):
                        try:
                            yield json.loads( ''.join(read_characters) )
                        except:
                            print("ERROR: failure when loading json.")
                            print("The original text to load as json:")
                            print(''.join(read_characters).replace("{", "{!!!"))
                            exit(1)

                    



