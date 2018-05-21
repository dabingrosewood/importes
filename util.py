def timetransfer(second):
    return 1519858800+second

def analyze(string):
    listform=string.replace('\n',"").split(",")
    for i in range(len(listform)):
        if (listform[i] == "?"):
            listform[i] = "unknown"
        if (listform[i].isdigit()):
            listform[i] = int(listform[i])

    return tuple(listform)