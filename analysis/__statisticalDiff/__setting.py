class significanceStars:
    SIGN_CODES = "\n\nSignif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1"
    
    def __init__(self) -> None:
        pass

    @classmethod
    def sign(cls, p: float) -> str:

        return "***" if p < 0.001 else "**" if p < 0.01 else "*" if p < 0.05 else "." if p < 0.1 else ""