class ErroServidor(Exception):
    def __init__(self, message='Erro no servidor'):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

class ErroParametroProtocoloInvalido(Exception):
    def __init__(self, message="Parâmetro 'PROTOCOLO' inválido. O parâmetro deve estar no formato: TJPE<número com 26 caracteres>."):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

class ErroParametroPesquisaRecebimento(Exception):
    def __init__(self, message='É necessário informar um PROTOCOLO ou COMPETÊNCIA!'):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
            
class ErroVigenciaInexistente(Exception):
    def __init__(self, message='Vigência não existente no ORAPROD!'):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)