Nesse diretório ficam os arquivos de configuração utilizados pelo script de deploy no openshift do Jenkins

Cada linha correponde a um projeto/build para deploy.
O script le o arquivo 06-Configuracao\openshift\%Profile%.txt e faz o deploy no openshift de cada linha encontrada.

Cada linha deve contem 4 parametros separados por virgula: 

1. Projeto do openshift
2. Configbuild do openshift;
3. Diretório do subprojeto java (normalmente o ear ou war final para o deploy);
4. Nome do projeto no GIT do sistema criado pela GEDAI;
5. Nome do build para deploy (nome final do arquivo gerado. Aceita a variavel ${VersaoBuild}).
