import pandas as pd

def aNode(kind, key, constitution, essence):

    thisNode = f"CREATE ({key}:{kind} {{{trueEssence(kind, constitution, essence)}}})"
    print(thisNode)

def trueEssence(kind, constitution, essence):
    theObject = dict(zip(constitution, essence))
    if kind == "Band":
        return("This a band")
    elif kind == "Artist":
        return("This is an Artist")
    elif kind == "Album":
        return("Quite an Album you got there")
    elif kind == "Genre":
        return("So much genres")
    else:
        return("sup")
aNode("Genre", "ola", "a","a")