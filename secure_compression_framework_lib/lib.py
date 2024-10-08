class Principal:
    """
    Simple generic Principal class. To be instantiated with a relevant set of attributes, potentially with a bridge to a database.
    
    Can be instantiated with a dictionary of attributes. Subsequent attributes can be set/updated with p[k] = v, for a principal object P. 
    """
    def __init__(self, **attr):
        for k,v in attr.items():
            setattr(self, k, v)
        return
    
    def __setitem__(self, k, v):
        self.k = v
        return

    def __getitem__(self, k):
        return self.k