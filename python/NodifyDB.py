import pandas as pd

def createNode(kind, KIA, constitution = None, essence = None, oneInstance = False):
    '''
    Requires:
        kind : Node shape (Natively supports: Band;Artist;Album;Genre)
        KIA : Key Identification Accessor (Uniqueness is assumed)
        constitution : Node composition
        essence : Composition information
        oneInstance : | False if this is a line in query with more entries
                      | True if this is a single line to be inserted
    '''
    if oneInstance == True:
        create = "CREATE "
    else:
        create = ""
    
    if (constitution != None and essence != None and (len(constitution) == len(essence))):
        thisNode = f'({KIA}:{kind}' + " {"
        builder = ', '.join([f'{constitution[i]}: "{essence[i]}"' for i in range(0, len(constitution))])
        return(create + thisNode + builder + '})')
    else:
        return(create + f'({KIA}:{kind})')


def createLinkage(kiaNodeA, kiaNodeB, verb, attributes = None, meaning = None, oneInstance = False):
    '''
    Requires:
        kiaNodeA / kiaNodeB : Key Identification Accessor of nodes A and B.
        verb : The connection between these two nodes.
        attributes: Information related to this linkage.
        meaning: Values associated with the attributes.
        oneInstance : | False if this is a line in query with more entries
                      | True if this is a single line to be inserted
    '''
    nodeA = f'({kiaNodeA})-'
    nodeB = f'->({kiaNodeB})'

    if oneInstance == True:
        create = "CREATE "
    else:
        create = ""
    
    if (attributes != None and meaning != None and (len(attributes) == len(meaning))):
        theVerbA = f'[:{verb}'
        builder = ', '.join([f'{attributes[i]}: "{meaning[i]}"' for i in range(0, len(attributes))])
        return(create + nodeA + theVerbA + ' {' + builder + '}]' + nodeB)
    else:
        return(create + nodeA + f'[:{verb}]' + nodeB)

#Test queries
#print(createNode("Human", "Human1", ["name","age"],["Joseph",44]))
#print(createNode("House", "House1", ["name"], ["Stallosholmet"]))
#print(createNode("HouseDivison", "Kitchen"))
#print(createLinkage("Human1", "House1", "lives"))
#print(createLinkage("House1", "Kitchen", "has", ["since"], ["built, in 1967"]))