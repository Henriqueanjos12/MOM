#!/usr/bin/env python3
"""
Sistema MOM - Message-Oriented Middleware com RabbitMQ
Gerenciador/Broker - Interface administrativa para gerenciar filas, tópicos e usuários

Autor: [Luiz Henrique]
Data: [04/08/2025]
Versão: 2.0

Funcionalidades:
- Gerenciamento de filas
- Gerenciamento de tópicos (exchanges)
- Gerenciamento de usuários e suas assinaturas
- Interface gráfica administrativa
"""

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import pika
import requests
from requests.auth import HTTPBasicAuth
from typing import Set, Dict, List, Optional, Tuple
import threading
import time


class ConfiguracaoRabbitMQ:
    """Configurações de conexão com RabbitMQ"""

    HOST = 'localhost'
    PORT = 5672
    MANAGEMENT_PORT = 15672
    USERNAME = 'guest'
    PASSWORD = 'guest'
    VIRTUAL_HOST = '/'


class GerenciadorRabbitMQ:
    """
    Classe para gerenciar conexões e operações com RabbitMQ

    Responsabilidades:
    - Gerenciar conexão com RabbitMQ
    - Operações CRUD em filas e exchanges
    - Validações através da API REST
    """

    def __init__(self):
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self._conectado = False

    def conectar(self) -> bool:
        """
        Estabelece conexão com RabbitMQ

        Returns:
            bool: True se conexão bem-sucedida, False caso contrário
        """
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=ConfiguracaoRabbitMQ.HOST,
                    port=ConfiguracaoRabbitMQ.PORT,
                    virtual_host=ConfiguracaoRabbitMQ.VIRTUAL_HOST
                )
            )
            self.channel = self.connection.channel()
            self._conectado = True
            return True

        except Exception as e:
            print(f"Erro ao conectar com RabbitMQ: {e}")
            self._conectado = False
            return False

    def desconectar(self) -> None:
        """Fecha a conexão com RabbitMQ de forma segura"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
        except Exception as e:
            print(f"Erro ao desconectar: {e}")
        finally:
            self._conectado = False

    def esta_conectado(self) -> bool:
        """Verifica se está conectado ao RabbitMQ"""
        return self._conectado and self.connection and not self.connection.is_closed

    def criar_fila(self, nome_fila: str, duravel: bool = True) -> Tuple[bool, str]:
        """
        Cria uma fila no RabbitMQ

        Args:
            nome_fila: Nome da fila a criar
            duravel: Se a fila deve sobreviver a reinicializações

        Returns:
            Tuple[bool, str]: (sucesso, mensagem)
        """
        try:
            if not self.esta_conectado():
                return False, "Não conectado ao RabbitMQ"

            self.channel.queue_declare(queue=nome_fila, durable=duravel)
            return True, f"Fila '{nome_fila}' criada com sucesso"

        except Exception as e:
            return False, f"Erro ao criar fila: {e}"

    def remover_fila(self, nome_fila: str) -> Tuple[bool, str]:
        """
        Remove uma fila do RabbitMQ

        Args:
            nome_fila: Nome da fila a remover

        Returns:
            Tuple[bool, str]: (sucesso, mensagem)
        """
        try:
            if not self.esta_conectado():
                return False, "Não conectado ao RabbitMQ"

            self.channel.queue_delete(queue=nome_fila)
            return True, f"Fila '{nome_fila}' removida com sucesso"

        except Exception as e:
            return False, f"Erro ao remover fila: {e}"

    def criar_topico(self, nome_topico: str, duravel: bool = True) -> Tuple[bool, str]:
        """
        Cria um tópico (exchange fanout) no RabbitMQ

        Args:
            nome_topico: Nome do tópico a criar
            duravel: Se o tópico deve sobreviver a reinicializações

        Returns:
            Tuple[bool, str]: (sucesso, mensagem)
        """
        try:
            if not self.esta_conectado():
                return False, "Não conectado ao RabbitMQ"

            self.channel.exchange_declare(
                exchange=nome_topico,
                exchange_type="fanout",
                durable=duravel
            )
            return True, f"Tópico '{nome_topico}' criado com sucesso"

        except Exception as e:
            return False, f"Erro ao criar tópico: {e}"

    def remover_topico(self, nome_topico: str) -> Tuple[bool, str]:
        """
        Remove um tópico (exchange) do RabbitMQ

        Args:
            nome_topico: Nome do tópico a remover

        Returns:
            Tuple[bool, str]: (sucesso, mensagem)
        """
        try:
            if not self.esta_conectado():
                return False, "Não conectado ao RabbitMQ"

            self.channel.exchange_delete(exchange=nome_topico)
            return True, f"Tópico '{nome_topico}' removido com sucesso"

        except Exception as e:
            return False, f"Erro ao remover tópico: {e}"

    def assinar_usuario_topico(self, usuario: str, topico: str) -> Tuple[bool, str]:
        """
        Inscreve um usuário em um tópico

        Args:
            usuario: Nome do usuário
            topico: Nome do tópico

        Returns:
            Tuple[bool, str]: (sucesso, mensagem)
        """
        try:
            if not self.esta_conectado():
                return False, "Não conectado ao RabbitMQ"

            # Garantir que o exchange existe
            self.channel.exchange_declare(exchange=topico, exchange_type='fanout', durable=True)

            # Criar fila específica para o usuário no tópico
            fila_topico = f"topic_{topico}_{usuario}"
            self.channel.queue_declare(queue=fila_topico, durable=True)

            # Vincular fila ao exchange
            self.channel.queue_bind(exchange=topico, queue=fila_topico)

            return True, f"Usuário '{usuario}' inscrito no tópico '{topico}'"

        except Exception as e:
            return False, f"Erro ao inscrever usuário: {e}"

    def desassinar_usuario_topico(self, usuario: str, topico: str) -> Tuple[bool, str]:
        """
        Remove inscrição de um usuário de um tópico

        Args:
            usuario: Nome do usuário
            topico: Nome do tópico

        Returns:
            Tuple[bool, str]: (sucesso, mensagem)
        """
        try:
            if not self.esta_conectado():
                return False, "Não conectado ao RabbitMQ"

            fila_topico = f"topic_{topico}_{usuario}"
            self.channel.queue_delete(queue=fila_topico)

            return True, f"Usuário '{usuario}' removido do tópico '{topico}'"

        except Exception as e:
            return False, f"Erro ao remover inscrição: {e}"


class ConsultorRabbitMQ:
    """
    Classe para consultas na API REST do RabbitMQ

    Responsabilidades:
    - Listar filas, exchanges e bindings
    - Verificar status e estatísticas
    - Validar existência de recursos
    """

    def __init__(self):
        self.base_url = f"http://{ConfiguracaoRabbitMQ.HOST}:{ConfiguracaoRabbitMQ.MANAGEMENT_PORT}/api"
        self.auth = HTTPBasicAuth(ConfiguracaoRabbitMQ.USERNAME, ConfiguracaoRabbitMQ.PASSWORD)

    def listar_filas(self) -> List[Dict]:
        """
        Lista todas as filas do RabbitMQ

        Returns:
            List[Dict]: Lista de informações das filas
        """
        try:
            response = requests.get(f"{self.base_url}/queues", auth=self.auth, timeout=5)
            if response.status_code == 200:
                # Filtrar filas do sistema (que começam com "amq.")
                return [fila for fila in response.json() if not fila['name'].startswith("amq.")]
            return []
        except Exception as e:
            print(f"Erro ao listar filas: {e}")
            return []

    def listar_exchanges(self) -> List[Dict]:
        """
        Lista todos os exchanges do RabbitMQ

        Returns:
            List[Dict]: Lista de informações dos exchanges
        """
        try:
            response = requests.get(f"{self.base_url}/exchanges", auth=self.auth, timeout=5)
            if response.status_code == 200:
                # Filtrar exchanges do sistema e retornar apenas fanout
                return [
                    ex for ex in response.json()
                    if ex['type'] == 'fanout' and not ex['name'].startswith("amq.")
                ]
            return []
        except Exception as e:
            print(f"Erro ao listar exchanges: {e}")
            return []

    def verificar_assinatura_existe(self, usuario: str, topico: str) -> bool:
        """
        Verifica se um usuário está inscrito em um tópico

        Args:
            usuario: Nome do usuário
            topico: Nome do tópico

        Returns:
            bool: True se inscrito, False caso contrário
        """
        try:
            fila_topico = f"topic_{topico}_{usuario}"
            filas = self.listar_filas()
            return any(fila['name'] == fila_topico for fila in filas)
        except Exception:
            return False


class MOMGerenciador:
    """
    Classe principal do Gerenciador MOM

    Responsabilidades:
    - Interface gráfica administrativa
    - Coordenar operações entre RabbitMQ e interface
    - Gerenciar estado da aplicação
    """

    def __init__(self):
        self.root = tk.Tk()
        self._configurar_janela_principal()

        # Componentes de gerenciamento
        self.gerenciador_rabbitmq = GerenciadorRabbitMQ()
        self.consultor_rabbitmq = ConsultorRabbitMQ()

        # Estado da aplicação
        self.usuarios: Set[str] = set()
        self.check_vars: Dict[str, Dict[str, tk.BooleanVar]] = {}

        # Tentar conectar ao RabbitMQ
        if self._inicializar_conexao():
            self._carregar_usuarios_existentes()
            self._criar_interface()
        else:
            messagebox.showerror("Erro", "Não foi possível conectar ao RabbitMQ.")
            self.root.destroy()

    def _configurar_janela_principal(self) -> None:
        """Configura as propriedades da janela principal"""
        self.root.title("Gerenciador MOM - RabbitMQ")
        self.root.geometry("1000x600")
        self.root.protocol("WM_DELETE_WINDOW", self._fechar_aplicacao)

        # Centralizar janela
        self.root.update_idletasks()
        x = (self.root.winfo_screenwidth() // 2) - (1000 // 2)
        y = (self.root.winfo_screenheight() // 2) - (600 // 2)
        self.root.geometry(f"1000x600+{x}+{y}")

    def _inicializar_conexao(self) -> bool:
        """
        Inicializa conexão com RabbitMQ

        Returns:
            bool: True se conexão bem-sucedida
        """
        return self.gerenciador_rabbitmq.conectar()

    def _criar_interface(self) -> None:
        """Cria e organiza a interface gráfica principal"""
        frame_principal = ttk.Frame(self.root, padding="10")
        frame_principal.pack(fill=tk.BOTH, expand=True)

        # Título
        titulo = ttk.Label(
            frame_principal,
            text="Gerenciador MOM",
            font=("Arial", 16, "bold")
        )
        titulo.pack(pady=10)

        # Notebook com abas
        self.notebook = ttk.Notebook(frame_principal)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=10)

        # Criar abas
        self._criar_aba_filas()
        self._criar_aba_topicos()
        self._criar_aba_usuarios()

        # Barra de status
        self.status_label = ttk.Label(
            frame_principal,
            text="Conectado ao RabbitMQ",
            foreground="green"
        )
        self.status_label.pack(pady=5)

    def _criar_aba_filas(self) -> None:
        """Cria a aba de gerenciamento de filas"""
        aba_filas = ttk.Frame(self.notebook)
        self.notebook.add(aba_filas, text="Filas")

        frame = ttk.Frame(aba_filas, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        # Título da seção
        ttk.Label(
            frame,
            text="Gerenciamento de Filas",
            font=("Arial", 12, "bold")
        ).pack(anchor=tk.W, pady=(0, 10))

        # Frame de entrada e botões
        frame_entrada = ttk.Frame(frame)
        frame_entrada.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame_entrada, text="Nome da Fila:").pack(anchor=tk.W)
        self.entry_fila = ttk.Entry(frame_entrada, width=40, font=('Arial', 10))
        self.entry_fila.pack(fill=tk.X, pady=(0, 5))

        # Botões de ação
        frame_botoes_fila = ttk.Frame(frame_entrada)
        frame_botoes_fila.pack(fill=tk.X, pady=5)

        ttk.Button(
            frame_botoes_fila,
            text="Adicionar Fila",
            command=self._adicionar_fila
        ).pack(side=tk.LEFT, padx=(0, 5))

        ttk.Button(
            frame_botoes_fila,
            text="Remover Fila",
            command=self._remover_fila
        ).pack(side=tk.LEFT, padx=(0, 5))

        ttk.Button(
            frame_botoes_fila,
            text="Atualizar Lista",
            command=self._listar_filas
        ).pack(side=tk.LEFT)

        # Lista de filas
        frame_lista_filas = ttk.Frame(frame)
        frame_lista_filas.pack(fill=tk.BOTH, expand=True)

        # Scrollbar para a lista
        scrollbar_filas = ttk.Scrollbar(frame_lista_filas)
        scrollbar_filas.pack(side=tk.RIGHT, fill=tk.Y)

        self.lista_filas = tk.Listbox(
            frame_lista_filas,
            height=12,
            font=('Arial', 10),
            yscrollcommand=scrollbar_filas.set
        )
        self.lista_filas.pack(fill=tk.BOTH, expand=True)
        self.lista_filas.bind("<<ListboxSelect>>", self._selecionar_fila)

        scrollbar_filas.config(command=self.lista_filas.yview)

        # Carregar filas iniciais
        self._listar_filas()

    def _criar_aba_topicos(self) -> None:
        """Cria a aba de gerenciamento de tópicos"""
        aba_topicos = ttk.Frame(self.notebook)
        self.notebook.add(aba_topicos, text="Tópicos")

        frame = ttk.Frame(aba_topicos, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        # Título da seção
        ttk.Label(
            frame,
            text="Gerenciamento de Tópicos",
            font=("Arial", 12, "bold")
        ).pack(anchor=tk.W, pady=(0, 10))

        # Frame de entrada e botões
        frame_entrada = ttk.Frame(frame)
        frame_entrada.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame_entrada, text="Nome do Tópico:").pack(anchor=tk.W)
        self.entry_topico = ttk.Entry(frame_entrada, width=40, font=('Arial', 10))
        self.entry_topico.pack(fill=tk.X, pady=(0, 5))

        # Botões de ação
        frame_botoes_topico = ttk.Frame(frame_entrada)
        frame_botoes_topico.pack(fill=tk.X, pady=5)

        ttk.Button(
            frame_botoes_topico,
            text="Adicionar Tópico",
            command=self._adicionar_topico
        ).pack(side=tk.LEFT, padx=(0, 5))

        ttk.Button(
            frame_botoes_topico,
            text="Remover Tópico",
            command=self._remover_topico
        ).pack(side=tk.LEFT, padx=(0, 5))

        ttk.Button(
            frame_botoes_topico,
            text="Atualizar Lista",
            command=self._listar_topicos
        ).pack(side=tk.LEFT)

        # Lista de tópicos
        frame_lista_topicos = ttk.Frame(frame)
        frame_lista_topicos.pack(fill=tk.BOTH, expand=True)

        # Scrollbar para a lista
        scrollbar_topicos = ttk.Scrollbar(frame_lista_topicos)
        scrollbar_topicos.pack(side=tk.RIGHT, fill=tk.Y)

        self.lista_topicos = tk.Listbox(
            frame_lista_topicos,
            height=12,
            font=('Arial', 10),
            yscrollcommand=scrollbar_topicos.set
        )
        self.lista_topicos.pack(fill=tk.BOTH, expand=True)
        self.lista_topicos.bind("<<ListboxSelect>>", self._selecionar_topico)

        scrollbar_topicos.config(command=self.lista_topicos.yview)

        # Carregar tópicos iniciais
        self._listar_topicos()

    def _criar_aba_usuarios(self) -> None:
        """Cria a aba de gerenciamento de usuários e assinaturas"""
        aba_usuarios = ttk.Frame(self.notebook)
        self.notebook.add(aba_usuarios, text="Usuários")

        frame = ttk.Frame(aba_usuarios, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        # Título da seção
        ttk.Label(
            frame,
            text="Gerenciamento de Usuários e Assinaturas",
            font=("Arial", 12, "bold")
        ).pack(anchor=tk.W, pady=(0, 10))

        # Botões de controle
        frame_botoes_usuario = ttk.Frame(frame)
        frame_botoes_usuario.pack(fill=tk.X, pady=(0, 10))

        ttk.Button(
            frame_botoes_usuario,
            text="Adicionar Usuário",
            command=self._adicionar_usuario
        ).pack(side=tk.LEFT, padx=(0, 5))

        ttk.Button(
            frame_botoes_usuario,
            text="Remover Usuário",
            command=self._remover_usuario
        ).pack(side=tk.LEFT, padx=(0, 5))

        ttk.Button(
            frame_botoes_usuario,
            text="Atualizar Tabela",
            command=self._atualizar_tabela_assinaturas
        ).pack(side=tk.LEFT)

        # Frame para tabela de assinaturas com scroll
        frame_tabela_container = ttk.Frame(frame)
        frame_tabela_container.pack(fill=tk.BOTH, expand=True)

        # Canvas e scrollbars para tabela scrollável
        canvas = tk.Canvas(frame_tabela_container)
        scrollbar_v = ttk.Scrollbar(frame_tabela_container, orient="vertical", command=canvas.yview)
        scrollbar_h = ttk.Scrollbar(frame_tabela_container, orient="horizontal", command=canvas.xview)

        self.tabela_frame = ttk.Frame(canvas)

        # Configurar scrolling
        canvas.configure(yscrollcommand=scrollbar_v.set, xscrollcommand=scrollbar_h.set)
        canvas.create_window((0, 0), window=self.tabela_frame, anchor="nw")

        # Pack dos componentes
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar_v.pack(side="right", fill="y")
        scrollbar_h.pack(side="bottom", fill="x")

        # Atualizar região scrollável quando conteúdo mudar
        self.tabela_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        # Carregar tabela inicial
        self._atualizar_tabela_assinaturas()

    # ====== MÉTODOS DE GERENCIAMENTO DE FILAS ======

    def _adicionar_fila(self) -> None:
        """Adiciona uma nova fila ao RabbitMQ"""
        nome_fila = self.entry_fila.get().strip()

        if not nome_fila:
            messagebox.showwarning("Aviso", "Digite um nome para a fila.")
            return

        if not self._validar_nome_recurso(nome_fila):
            messagebox.showwarning(
                "Aviso",
                "Nome inválido. Use apenas letras, números, hífens e underscores."
            )
            return

        sucesso, mensagem = self.gerenciador_rabbitmq.criar_fila(nome_fila)

        if sucesso:
            messagebox.showinfo("Sucesso", mensagem)
            self.entry_fila.delete(0, tk.END)
            self._listar_filas()
        else:
            messagebox.showerror("Erro", mensagem)

    def _remover_fila(self) -> None:
        """Remove uma fila do RabbitMQ"""
        nome_fila = self.entry_fila.get().strip()

        if not nome_fila:
            messagebox.showwarning("Aviso", "Digite o nome da fila a remover.")
            return

        # Verificar se é fila de usuário (proteção)
        if nome_fila.startswith("user_"):
            messagebox.showwarning(
                "Aviso",
                "Não é permitido remover a fila de um usuário diretamente!\n"
                "Use a aba Usuários para remover o usuário correspondente."
            )
            return

        # Confirmação
        if not messagebox.askyesno(
                "Confirmação",
                f"Tem certeza que deseja remover a fila '{nome_fila}'?"
        ):
            return

        sucesso, mensagem = self.gerenciador_rabbitmq.remover_fila(nome_fila)

        if sucesso:
            messagebox.showinfo("Sucesso", mensagem)
            self.entry_fila.delete(0, tk.END)
            self._listar_filas()
        else:
            messagebox.showerror("Erro", mensagem)

    def _listar_filas(self) -> None:
        """Atualiza a lista de filas na interface"""
        self.lista_filas.delete(0, tk.END)

        filas = self.consultor_rabbitmq.listar_filas()
        filas_ordenadas = sorted(filas, key=lambda x: x['name'])

        for fila in filas_ordenadas:
            nome = fila['name']
            mensagens = fila.get('messages', 0)
            consumidores = fila.get('consumers', 0)

            # Formatação da linha
            status_line = f"{nome} - {mensagens} msgs, {consumidores} consumers"
            self.lista_filas.insert(tk.END, status_line)

    def _selecionar_fila(self, event) -> None:
        """Preenche o campo de entrada com a fila selecionada"""
        selecao = self.lista_filas.curselection()
        if selecao:
            texto_completo = self.lista_filas.get(selecao[0])
            nome_fila = texto_completo.split(" - ")[0]

            self.entry_fila.delete(0, tk.END)
            self.entry_fila.insert(0, nome_fila)

    # ====== MÉTODOS DE GERENCIAMENTO DE TÓPICOS ======

    def _adicionar_topico(self) -> None:
        """Adiciona um novo tópico ao RabbitMQ"""
        nome_topico = self.entry_topico.get().strip()

        if not nome_topico:
            messagebox.showwarning("Aviso", "Digite um nome para o tópico.")
            return

        if not self._validar_nome_recurso(nome_topico):
            messagebox.showwarning(
                "Aviso",
                "Nome inválido. Use apenas letras, números, hífens e underscores."
            )
            return

        sucesso, mensagem = self.gerenciador_rabbitmq.criar_topico(nome_topico)

        if sucesso:
            messagebox.showinfo("Sucesso", mensagem)
            self.entry_topico.delete(0, tk.END)
            self._listar_topicos()
            # Atualizar tabela de assinaturas pois novo tópico foi criado
            self._atualizar_tabela_assinaturas()
        else:
            messagebox.showerror("Erro", mensagem)

    def _remover_topico(self) -> None:
        """Remove um tópico do RabbitMQ"""
        nome_topico = self.entry_topico.get().strip()

        if not nome_topico:
            messagebox.showwarning("Aviso", "Digite o nome do tópico a remover.")
            return

        # Confirmação
        if not messagebox.askyesno(
                "Confirmação",
                f"Tem certeza que deseja remover o tópico '{nome_topico}'?\n"
                "Todas as assinaturas serão perdidas."
        ):
            return

        sucesso, mensagem = self.gerenciador_rabbitmq.remover_topico(nome_topico)

        if sucesso:
            messagebox.showinfo("Sucesso", mensagem)
            self.entry_topico.delete(0, tk.END)
            self._listar_topicos()
            # Atualizar tabela de assinaturas
            self._atualizar_tabela_assinaturas()
        else:
            messagebox.showerror("Erro", mensagem)

    def _listar_topicos(self) -> None:
        """Atualiza a lista de tópicos na interface"""
        self.lista_topicos.delete(0, tk.END)

        exchanges = self.consultor_rabbitmq.listar_exchanges()
        exchanges_ordenados = sorted(exchanges, key=lambda x: x['name'])

        for exchange in exchanges_ordenados:
            nome = exchange['name']
            self.lista_topicos.insert(tk.END, nome)

    def _selecionar_topico(self, event) -> None:
        """Preenche o campo de entrada com o tópico selecionado"""
        selecao = self.lista_topicos.curselection()
        if selecao:
            nome_topico = self.lista_topicos.get(selecao[0])

            self.entry_topico.delete(0, tk.END)
            self.entry_topico.insert(0, nome_topico)

    # ====== MÉTODOS DE GERENCIAMENTO DE USUÁRIOS ======

    def _adicionar_usuario(self) -> None:
        """Adiciona um novo usuário ao sistema"""
        nome_usuario = simpledialog.askstring(
            "Novo Usuário",
            "Digite o nome do usuário:",
            parent=self.root
        )

        if not nome_usuario:
            return

        nome_usuario = nome_usuario.strip()

        if not self._validar_nome_recurso(nome_usuario):
            messagebox.showwarning(
                "Aviso",
                "Nome inválido. Use apenas letras, números, hífens e underscores."
            )
            return

        if nome_usuario in self.usuarios:
            messagebox.showwarning("Aviso", "Usuário já existe!")
            return

        # Criar fila pessoal do usuário
        fila_pessoal = f"user_{nome_usuario}"
        sucesso, mensagem = self.gerenciador_rabbitmq.criar_fila(fila_pessoal)

        if sucesso:
            self.usuarios.add(nome_usuario)
            messagebox.showinfo(
                "Sucesso",
                f"Usuário '{nome_usuario}' criado com fila pessoal '{fila_pessoal}'"
            )
            self._atualizar_tabela_assinaturas()
        else:
            messagebox.showerror("Erro", f"Falha ao criar usuário: {mensagem}")

    def _remover_usuario(self) -> None:
        """Remove um usuário e todas suas assinaturas"""
        if not self.usuarios:
            messagebox.showwarning("Aviso", "Nenhum usuário para remover!")
            return

        # Seleção do usuário a remover
        usuarios_lista = sorted(list(self.usuarios))
        dialog = ListSelectionDialog(
            self.root,
            "Remover Usuário",
            "Selecione o usuário a remover:",
            usuarios_lista
        )

        nome_usuario = dialog.resultado
        if not nome_usuario:
            return

        # Confirmação
        if not messagebox.askyesno(
                "Confirmação",
                f"Tem certeza que deseja remover o usuário '{nome_usuario}'?\n"
                "Todas as suas filas e assinaturas serão removidas."
        ):
            return

        try:
            # Remover fila pessoal
            fila_pessoal = f"user_{nome_usuario}"
            self.gerenciador_rabbitmq.remover_fila(fila_pessoal)

            # Remover todas as filas de tópicos do usuário
            filas = self.consultor_rabbitmq.listar_filas()
            for fila in filas:
                nome_fila = fila['name']
                if nome_fila.endswith(f"_{nome_usuario}") and nome_fila.startswith("topic_"):
                    self.gerenciador_rabbitmq.remover_fila(nome_fila)

            # Remover usuário do conjunto
            self.usuarios.discard(nome_usuario)

            messagebox.showinfo("Sucesso", f"Usuário '{nome_usuario}' removido com sucesso!")
            self._atualizar_tabela_assinaturas()

        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao remover usuário: {e}")

    def _atualizar_tabela_assinaturas(self) -> None:
        """Atualiza a tabela de assinaturas usuário x tópico"""
        # Limpar tabela atual
        for widget in self.tabela_frame.winfo_children():
            widget.destroy()

        usuarios_ordenados = sorted(list(self.usuarios))
        topicos_disponiveis = [ex['name'] for ex in self.consultor_rabbitmq.listar_exchanges()]
        topicos_ordenados = sorted(topicos_disponiveis)

        # Se não há usuários ou tópicos, mostrar mensagem
        if not usuarios_ordenados or not topicos_ordenados:
            ttk.Label(
                self.tabela_frame,
                text="Crie usuários e tópicos para gerenciar assinaturas",
                font=('Arial', 12)
            ).grid(row=0, column=0, padx=20, pady=20)
            return

        # Cabeçalho da tabela
        ttk.Label(
            self.tabela_frame,
            text="Usuário",
            width=20,
            anchor="center",
            font=('Arial', 10, 'bold')
        ).grid(row=0, column=0, padx=5, pady=5, sticky="nsew")

        for j, topico in enumerate(topicos_ordenados, start=1):
            ttk.Label(
                self.tabela_frame,
                text=topico,
                width=15,
                anchor="center",
                font=('Arial', 10, 'bold')
            ).grid(row=0, column=j, padx=5, pady=5, sticky="nsew")

        # Linhas da tabela
        self.check_vars = {}
        for i, usuario in enumerate(usuarios_ordenados, start=1):
            ttk.Label(
                self.tabela_frame,
                text=usuario,
                width=20,
                font=('Arial', 10)
            ).grid(row=i, column=0, padx=5, pady=5, sticky="w")

            self.check_vars[usuario] = {}

            for j, topico in enumerate(topicos_ordenados, start=1):
                # Verificar se usuário está inscrito no tópico
                inscrito = self.consultor_rabbitmq.verificar_assinatura_existe(usuario, topico)

                var = tk.BooleanVar(value=inscrito)
                checkbox = ttk.Checkbutton(
                    self.tabela_frame,
                    variable=var,
                    command=lambda u=usuario, t=topico, v=var: self._toggle_assinatura(u, t, v)
                )
                checkbox.grid(row=i, column=j, padx=5, pady=5)
                self.check_vars[usuario][topico] = var

    def _toggle_assinatura(self, usuario: str, topico: str, var: tk.BooleanVar) -> None:
        """Alterna a assinatura de um usuário em um tópico"""
        if var.get():
            # Inscrever usuário
            sucesso, mensagem = self.gerenciador_rabbitmq.assinar_usuario_topico(usuario, topico)
            if sucesso:
                messagebox.showinfo("Sucesso", mensagem)
            else:
                # Reverter checkbox em caso de erro
                var.set(False)
                messagebox.showerror("Erro", mensagem)
        else:
            # Desinscrever usuário
            sucesso, mensagem = self.gerenciador_rabbitmq.desassinar_usuario_topico(usuario, topico)
            if sucesso:
                messagebox.showinfo("Sucesso", mensagem)
            else:
                # Reverter checkbox em caso de erro
                var.set(True)
                messagebox.showerror("Erro", mensagem)

    def _carregar_usuarios_existentes(self) -> None:
        """Carrega usuários existentes a partir das filas user_*"""
        try:
            filas = self.consultor_rabbitmq.listar_filas()
            for fila in filas:
                nome_fila = fila['name']
                if nome_fila.startswith("user_"):
                    nome_usuario = nome_fila.replace("user_", "")
                    self.usuarios.add(nome_usuario)
        except Exception as e:
            print(f"Erro ao carregar usuários existentes: {e}")

    # ====== MÉTODOS UTILITÁRIOS ======

    def _validar_nome_recurso(self, nome: str) -> bool:
        """
        Valida se um nome de recurso é válido

        Args:
            nome: Nome a validar

        Returns:
            bool: True se válido, False caso contrário
        """
        import re
        # Permite letras, números, hífens e underscores
        return bool(re.match(r'^[a-zA-Z0-9_-]+$', nome))

    def _fechar_aplicacao(self) -> None:
        """Fecha a aplicação de forma segura"""
        try:
            self.gerenciador_rabbitmq.desconectar()
        except Exception as e:
            print(f"Erro ao desconectar: {e}")
        finally:
            self.root.destroy()

    def executar(self) -> None:
        """Inicia o loop principal da aplicação"""
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            print("Aplicação fechada pelo usuário")
        except Exception as e:
            print(f"Erro na execução: {e}")
        finally:
            self._fechar_aplicacao()


class ListSelectionDialog:
    """
    Dialog para seleção de item de uma lista
    """

    def __init__(self, parent, titulo: str, mensagem: str, opcoes: List[str]):
        self.resultado = None

        # Criar janela modal
        self.dialog = tk.Toplevel(parent)
        self.dialog.title(titulo)
        self.dialog.geometry("300x400")
        self.dialog.transient(parent)
        self.dialog.grab_set()

        # Centralizar dialog
        self.dialog.update_idletasks()
        x = parent.winfo_x() + (parent.winfo_width() // 2) - 150
        y = parent.winfo_y() + (parent.winfo_height() // 2) - 200
        self.dialog.geometry(f"300x400+{x}+{y}")

        # Widgets
        ttk.Label(self.dialog, text=mensagem).pack(pady=10)

        frame_lista = ttk.Frame(self.dialog)
        frame_lista.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        scrollbar = ttk.Scrollbar(frame_lista)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.listbox = tk.Listbox(frame_lista, yscrollcommand=scrollbar.set)
        self.listbox.pack(fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.listbox.yview)

        # Preencher lista
        for opcao in opcoes:
            self.listbox.insert(tk.END, opcao)

        # Botões
        frame_botoes = ttk.Frame(self.dialog)
        frame_botoes.pack(pady=10)

        ttk.Button(frame_botoes, text="OK", command=self._ok).pack(side=tk.LEFT, padx=5)
        ttk.Button(frame_botoes, text="Cancelar", command=self._cancelar).pack(side=tk.LEFT, padx=5)

        # Aguardar resultado
        self.dialog.wait_window()

    def _ok(self):
        selecao = self.listbox.curselection()
        if selecao:
            self.resultado = self.listbox.get(selecao[0])
        self.dialog.destroy()

    def _cancelar(self):
        self.dialog.destroy()


def main():
    """Função principal - ponto de entrada da aplicação"""
    try:
        app = MOMGerenciador()
        app.executar()
    except Exception as e:
        print(f"Erro fatal na inicialização do gerenciador: {e}")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    main()