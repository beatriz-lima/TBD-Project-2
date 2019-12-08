import math


def helperWrite(value):
    if (type(value) == int or type(value) == float) and not math.isnan(value):
        return value
    else:
        return f'"{value}"'


def createNode(kind, KIA, constitution=None, essence=None, oneInstance=False):
    '''
    Requires:
        kind : Node shape (Natively supports: Band;Artist;Album;Genre)
        KIA : Key Identification Accessor (Uniqueness is assumed)
        constitution : Node composition
        essence : Composition information
        oneInstance : | False if this is a line in query with more entries
                      | True if this is a single line to be inserted
    '''
    create = "CREATE " if oneInstance else ""
    if kind == "ARTISTS" and type(essence[1]) == str:
        essence[1] = essence[1].replace(";", ",")

    if constitution is not None and essence is not None and (len(constitution) == len(essence)):
        thisNode = f'({KIA}:{kind}' + " {"
        builder = ', '.join([f'{constitution[i]}: {helperWrite(essence[i])}' for i in range(0, len(constitution))])
        return create + thisNode + builder + '})'
    else:
        return create + f'({KIA}:{kind})'


def createLinkage(connection, link, kiaNodeA, kiaNodeB, attributes=None, meaning=None, oneInstance=False):
    '''
    Requires:
        link : The type of relationship and between who
        kiaNodeA / kiaNodeB : Key Identification Accessor of nodes A and B.
        attributes: Information related to this linkage.
        meaning: Values associated with the attributes.
        oneInstance : | False if this is a line in query with more entries
                      | True if this is a single line to be inserted
    '''
    nodeA = f'(a)-'
    nodeB = f'->(b)'

    create = "CREATE " if oneInstance else ""

    string = f'MATCH (a:{link[0]}),(b:{link[1]}) WHERE a.id = {kiaNodeA} AND b.id = {kiaNodeB} '

    if attributes is not None and meaning is not None and (len(attributes) == len(meaning)):
        builder = ', '.join([f'{attributes[i]}: {helperWrite(meaning[i])}' for i in range(0, len(attributes))])
        query = string + create + nodeA + f'[r:{link[2]}' + ' {' + builder + '}]' + nodeB
    else:
        query = string + create + nodeA + f'[r:{link[2]}]' + nodeB

    connection.run(query)
    connection.commit()
