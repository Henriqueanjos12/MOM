#!/usr/bin/env python3
"""
Sistema MOM - Message-Oriented Middleware com RabbitMQ
Launcher Principal - Permite iniciar o gerenciador ou cliente facilmente

Autor: [Luiz Henrique]
Data: [04/08/2025]
Versão: 2.0

Dependências:
- Python 3.x
- pika (pip install pika)
- requests (pip install requests)
- RabbitMQ Server
"""

import sys
import subprocess
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import os
import pika
import requests
from requests.auth import HTTPBasicAuth
from typing import Optional, Tuple


class ConfiguracaoSistema:
    """Classe para gerenciar configurações do sistema"""

    RABBITMQ_HOST = 'localhost'
    RABBITMQ_PORT = 5672
    RABBITMQ_MANAGEMENT_PORT = 15672
    RABBITMQ_USER = 'guest'
    RABBITMQ_PASSWORD = 'guest'

    # Arquivos do sistema
    ARQUIVO_GERENCIADOR = "mom_broker.py"
    ARQUIVO_CLIENTE = "mom_usuario.py"


class MOMLauncher:
    """
    Classe principal do launcher do Sistema MOM

    Responsabilidades:
    - Verificar status do RabbitMQ
    - Verificar existência dos arquivos do sistema
    - Fornecer interface para iniciar componentes
    - Validar usuários antes do login
    """

    def __init__(self):
        """Inicializa o launcher e configura a interface"""
        self.root = tk.Tk()
        self._configurar_janela_principal()

        # Verificar status do sistema
        self.gerenciador_existe = self._verificar_arquivo_existe(ConfiguracaoSistema.ARQUIVO_GERENCIADOR)
        self.cliente_existe = self._verificar_arquivo_existe(ConfiguracaoSistema.ARQUIVO_CLIENTE)
        self.rabbitmq_ok = self._verificar_rabbitmq()

        self._criar_interface()

    def _configurar_janela_principal(self) -> None:
        """Configura as propriedades da janela principal"""
        self.root.title("MOM RabbitMQ System Launcher")
        self.root.geometry("500x400")
        self.root.resizable(False, False)

        # Centralizar janela na tela
        self.root.update_idletasks()
        x = (self.root.winfo_screenwidth() // 2) - (500 // 2)
        y = (self.root.winfo_screenheight() // 2) - (400 // 2)
        self.root.geometry(f"500x400+{x}+{y}")

    def _verificar_arquivo_existe(self, nome_arquivo: str) -> bool:
        """
        Verifica se um arquivo específico existe no diretório atual

        Args:
            nome_arquivo: Nome do arquivo a verificar

        Returns:
            bool: True se o arquivo existe, False caso contrário
        """
        return os.path.exists(nome_arquivo)

    def _verificar_rabbitmq(self) -> bool:
        """
        Verifica se o RabbitMQ está rodando e acessível

        Returns:
            bool: True se RabbitMQ está rodando, False caso contrário
        """
        try:
            # Tenta estabelecer conexão com RabbitMQ
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=ConfiguracaoSistema.RABBITMQ_HOST,
                    port=ConfiguracaoSistema.RABBITMQ_PORT
                )
            )
            connection.close()
            return True
        except Exception as e:
            print(f"Erro ao conectar com RabbitMQ: {e}")
            return False

    def _criar_interface(self) -> None:
        """Cria e organiza todos os elementos da interface gráfica"""
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        self._criar_cabecalho(main_frame)
        self._criar_secao_status(main_frame)
        self._criar_secao_instrucoes(main_frame)
        self._criar_botoes_principais(main_frame)
        self._criar_secao_informacoes(main_frame)

    def _criar_cabecalho(self, parent: ttk.Frame) -> None:
        """Cria o cabeçalho da aplicação"""
        titulo = ttk.Label(
            parent,
            text="Sistema MOM",
            font=('Arial', 18, 'bold')
        )
        titulo.pack(pady=(0, 5))

        subtitulo = ttk.Label(
            parent,
            text="Message-Oriented Middleware com RabbitMQ",
            font=('Arial', 10)
        )
        subtitulo.pack(pady=(0, 20))

    def _criar_secao_status(self, parent: ttk.Frame) -> None:
        """Cria a seção de status do sistema"""
        status_frame = ttk.LabelFrame(parent, text="Status do Sistema", padding="10")
        status_frame.pack(fill=tk.X, pady=(0, 20))

        # Status do RabbitMQ
        self._criar_status_rabbitmq(status_frame)

        # Status dos arquivos
        self._criar_status_arquivos(status_frame)

    def _criar_status_rabbitmq(self, parent: ttk.Frame) -> None:
        """Cria indicadores de status do RabbitMQ"""
        rabbitmq_status = "✓ Conectado" if self.rabbitmq_ok else "✗ Desconectado"
        rabbitmq_color = "green" if self.rabbitmq_ok else "red"

        status_rabbitmq = ttk.Label(
            parent,
            text=f"RabbitMQ: {rabbitmq_status}",
            foreground=rabbitmq_color,
            font=('Arial', 10, 'bold')
        )
        status_rabbitmq.pack(anchor=tk.W)

        # Se RabbitMQ não estiver rodando, mostrar aviso e botão de verificação
        if not self.rabbitmq_ok:
            aviso_label = ttk.Label(
                parent,
                text="⚠️ RabbitMQ não está rodando. Instale e inicie o RabbitMQ server.",
                foreground="red",
                font=('Arial', 9)
            )
            aviso_label.pack(anchor=tk.W, pady=(5, 0))

            btn_verificar = ttk.Button(
                parent,
                text="Verificar RabbitMQ Novamente",
                command=self._verificar_rabbitmq_novamente
            )
            btn_verificar.pack(pady=(10, 0))

    def _criar_status_arquivos(self, parent: ttk.Frame) -> None:
        """Cria indicadores de status dos arquivos do sistema"""
        gerenciador_status = "✓ Disponível" if self.gerenciador_existe else "✗ Não encontrado"
        cliente_status = "✓ Disponível" if self.cliente_existe else "✗ Não encontrado"

        ttk.Label(parent, text=f"Gerenciador: {gerenciador_status}").pack(anchor=tk.W)
        ttk.Label(parent, text=f"Cliente: {cliente_status}").pack(anchor=tk.W)

    def _criar_secao_instrucoes(self, parent: ttk.Frame) -> None:
        """Cria a seção de instruções de uso"""
        instrucoes_frame = ttk.LabelFrame(parent, text="Como usar", padding="10")
        instrucoes_frame.pack(fill=tk.X, pady=(0, 20))

        instrucoes_texto = """1. Certifique-se de que o RabbitMQ está rodando
2. Inicie o Gerenciador para configurar filas e tópicos
3. Inicie quantos Clientes precisar
4. Use as interfaces para enviar mensagens e gerenciar tópicos"""

        ttk.Label(
            instrucoes_frame,
            text=instrucoes_texto,
            font=('Arial', 9),
            justify=tk.LEFT
        ).pack(anchor=tk.W)

    def _criar_botoes_principais(self, parent: ttk.Frame) -> None:
        """Cria os botões principais de inicialização"""
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(pady=10)

        # Botão do Gerenciador
        self.btn_gerenciador = ttk.Button(
            btn_frame,
            text="Iniciar Gerenciador\n(Configuração)",
            command=self._iniciar_gerenciador,
            width=18
        )
        self.btn_gerenciador.pack(side=tk.LEFT, padx=(0, 10))

        # Desabilitar se não houver arquivo ou RabbitMQ não estiver rodando
        if not self.gerenciador_existe or not self.rabbitmq_ok:
            self.btn_gerenciador.config(state='disabled')

        # Botão do Cliente
        self.btn_cliente = ttk.Button(
            btn_frame,
            text="Iniciar Cliente\n(Usuário)",
            command=self._iniciar_cliente,
            width=18
        )
        self.btn_cliente.pack(side=tk.LEFT)

        # Desabilitar se não houver arquivo ou RabbitMQ não estiver rodando
        if not self.cliente_existe or not self.rabbitmq_ok:
            self.btn_cliente.config(state='disabled')

    def _criar_secao_informacoes(self, parent: ttk.Frame) -> None:
        """Cria a seção de informações sobre dependências"""
        info_frame = ttk.LabelFrame(parent, text="Informações", padding="10")
        info_frame.pack(fill=tk.X, pady=(20, 0))

        ttk.Label(info_frame, text="Dependências:", font=('Arial', 10, 'bold')).pack(anchor=tk.W)

        dependencias = [
            "• Python 3.x",
            "• pika (pip install pika)",
            "• requests (pip install requests)",
            "• RabbitMQ Server"
        ]

        for dep in dependencias:
            ttk.Label(info_frame, text=dep, font=('Arial', 9)).pack(anchor=tk.W)

    def _verificar_rabbitmq_novamente(self) -> None:
        """
        Reexecuta a verificação do RabbitMQ e atualiza a interface
        """
        self.rabbitmq_ok = self._verificar_rabbitmq()

        if self.rabbitmq_ok:
            messagebox.showinfo("Sucesso", "RabbitMQ está rodando!")
            # Recriar interface com novo status
            self._recriar_interface()
        else:
            messagebox.showerror(
                "Erro",
                "RabbitMQ ainda não está acessível.\n\n"
                "Verifique se está instalado e rodando."
            )

    def _recriar_interface(self) -> None:
        """Recria a interface com os status atualizados"""
        # Limpar widgets existentes
        for widget in self.root.winfo_children():
            widget.destroy()

        # Recriar interface
        self._criar_interface()

    def _iniciar_gerenciador(self) -> None:
        """
        Inicia o processo do gerenciador MOM

        Executa o arquivo mom_broker.py como processo separado
        """
        try:
            subprocess.Popen([sys.executable, ConfiguracaoSistema.ARQUIVO_GERENCIADOR])
            messagebox.showinfo("Sucesso", "Gerenciador iniciado com sucesso!")
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao iniciar gerenciador: {e}")

    def _iniciar_cliente(self) -> None:
        """
        Inicia um cliente apenas se o usuário já existir no sistema

        Processo:
        1. Solicita nome do usuário
        2. Valida se usuário existe no RabbitMQ
        3. Inicia cliente se válido
        """
        try:
            # Solicitar nome do usuário
            nome_usuario = simpledialog.askstring(
                "Login",
                "Digite seu nome de usuário:",
                parent=self.root
            )

            if not nome_usuario:
                return

            # Validar se usuário existe
            if not self._validar_usuario_existe(nome_usuario):
                messagebox.showerror(
                    "Erro",
                    f"O usuário '{nome_usuario}' não existe!\n"
                    "Peça ao administrador para criá-lo no Gerenciador."
                )
                return

            # Iniciar cliente
            subprocess.Popen([
                sys.executable,
                ConfiguracaoSistema.ARQUIVO_CLIENTE,
                nome_usuario
            ])

            messagebox.showinfo(
                "Sucesso",
                f"Cliente '{nome_usuario}' iniciado com sucesso!"
            )

        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao iniciar cliente: {e}")

    def _validar_usuario_existe(self, nome_usuario: str) -> bool:
        """
        Valida se um usuário existe no RabbitMQ através da API REST

        Args:
            nome_usuario: Nome do usuário a validar

        Returns:
            bool: True se usuário existe, False caso contrário
        """
        try:
            fila_pessoal = f"user_{nome_usuario}"

            # Consultar API REST do RabbitMQ
            url = f"http://{ConfiguracaoSistema.RABBITMQ_HOST}:{ConfiguracaoSistema.RABBITMQ_MANAGEMENT_PORT}/api/queues"
            response = requests.get(
                url,
                auth=HTTPBasicAuth(
                    ConfiguracaoSistema.RABBITMQ_USER,
                    ConfiguracaoSistema.RABBITMQ_PASSWORD
                ),
                timeout=5
            )

            if response.status_code == 200:
                filas = [fila['name'] for fila in response.json()]
                return fila_pessoal in filas
            else:
                print(f"Erro na API RabbitMQ: Status {response.status_code}")
                return False

        except Exception as e:
            print(f"Erro ao validar usuário: {e}")
            return False

    def executar(self) -> None:
        """
        Inicia o loop principal da aplicação
        """
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            print("Aplicação fechada pelo usuário")
        except Exception as e:
            print(f"Erro na execução da aplicação: {e}")


def main():
    """Função principal - ponto de entrada da aplicação"""
    try:
        app = MOMLauncher()
        app.executar()
    except Exception as e:
        print(f"Erro fatal na inicialização: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()