#!/usr/bin/env python3
"""
Sistema MOM - Message-Oriented Middleware com RabbitMQ
Cliente/Usuário - Interface para envio e recebimento de mensagens

Autor: [Luiz Henrique]
Data: [04/08/2025]
Versão: 2.0

Funcionalidades:
- Envio de mensagens diretas entre usuários
- Publicação e assinatura de tópicos
- Envio e recebimento em filas gerais
- Interface gráfica intuitiva
- Consumo assíncrono de mensagens
"""

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import pika
import json
import threading
import time
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
from typing import Optional, List, Dict, Callable, Tuple, Set
import sys


class ConfiguracaoRabbitMQ:
    """Configurações de conexão com RabbitMQ"""

    HOST = 'localhost'
    PORT = 5672
    MANAGEMENT_PORT = 15672
    USERNAME = 'guest'
    PASSWORD = 'guest'
    VIRTUAL_HOST = '/'


class TipoMensagem:
    """Constantes para tipos de mensagem"""

    MENSAGEM_DIRETA = 'mensagem_direta'
    MENSAGEM_TOPICO = 'mensagem_topico'
    MENSAGEM_FILA = 'mensagem_fila'


class RabbitMQCliente:
    """
    Classe para gerenciar conexão e operações do cliente RabbitMQ

    Responsabilidades:
    - Gerenciar conexões com RabbitMQ
    - Enviar mensagens (diretas, tópicos, filas)
    - Consumir mensagens assincronamente
    - Gerenciar assinaturas de tópicos
    """

    def __init__(self):
        # Conexões
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self._conectado = False

        # Estado do usuário
        self.nome_usuario = ""
        self.fila_pessoal = ""

        # Assinaturas e consumo
        self.topicos_assinados: Set[str] = set()
        self.callback_mensagem: Optional[Callable] = None
        self._consuming = False
        self._threads_consumo: List[threading.Thread] = []

    def conectar(self, nome_usuario: str) -> bool:
        """
        Conecta ao RabbitMQ e valida se o usuário existe

        Args:
            nome_usuario: Nome do usuário a conectar

        Returns:
            bool: True se conexão e validação bem-sucedidas
        """
        try:
            # Estabelecer conexão
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=ConfiguracaoRabbitMQ.HOST,
                    port=ConfiguracaoRabbitMQ.PORT,
                    virtual_host=ConfiguracaoRabbitMQ.VIRTUAL_HOST
                )
            )
            self.channel = self.connection.channel()

            # Configurar informações do usuário
            self.nome_usuario = nome_usuario
            self.fila_pessoal = f"user_{nome_usuario}"

            # Validar se usuário existe no sistema
            if not self._validar_usuario_existe():
                messagebox.showerror(
                    "Erro",
                    f"Usuário '{nome_usuario}' não existe!\n"
                    "Peça ao administrador para criá-lo no Gerenciador."
                )
                self.desconectar()
                return False

            self._conectado = True
            self._carregar_assinaturas_existentes()
            return True

        except Exception as e:
            print(f"Erro ao conectar: {e}")
            messagebox.showerror("Erro", f"Erro ao conectar ao RabbitMQ: {e}")
            return False

    def desconectar(self) -> None:
        """Desconecta do RabbitMQ de forma segura"""
        try:
            # Parar consumo
            self._consuming = False

            # Apenas sinaliza para parar, sem bloquear
            self._threads_consumo.clear()

            # Fechar conexão
            if self.connection and not self.connection.is_closed:
                self.connection.close()

        except Exception as e:
            print(f"Erro ao desconectar: {e}")
        finally:
            self._conectado = False

    def esta_conectado(self) -> bool:
        """Verifica se está conectado ao RabbitMQ"""
        return (self._conectado and
                self.connection and
                not self.connection.is_closed)

    def _validar_usuario_existe(self) -> bool:
        """
        Valida se o usuário existe através da API REST do RabbitMQ

        Returns:
            bool: True se usuário existe, False caso contrário
        """
        try:
            url = f"http://{ConfiguracaoRabbitMQ.HOST}:{ConfiguracaoRabbitMQ.MANAGEMENT_PORT}/api/queues"
            response = requests.get(
                url,
                auth=HTTPBasicAuth(ConfiguracaoRabbitMQ.USERNAME, ConfiguracaoRabbitMQ.PASSWORD),
                timeout=5
            )

            if response.status_code == 200:
                filas = [fila['name'] for fila in response.json()]
                return self.fila_pessoal in filas

            return False

        except Exception as e:
            print(f"Erro ao validar usuário: {e}")
            return False

    def _carregar_assinaturas_existentes(self) -> None:
        """Carrega assinaturas de tópicos existentes do usuário"""
        try:
            url = f"http://{ConfiguracaoRabbitMQ.HOST}:{ConfiguracaoRabbitMQ.MANAGEMENT_PORT}/api/queues"
            response = requests.get(
                url,
                auth=HTTPBasicAuth(ConfiguracaoRabbitMQ.USERNAME, ConfiguracaoRabbitMQ.PASSWORD),
                timeout=5
            )

            if response.status_code == 200:
                filas = response.json()
                for fila in filas:
                    nome_fila = fila['name']
                    # Verificar se é fila de tópico do usuário: topic_TOPICO_USUARIO
                    if (nome_fila.startswith("topic_") and
                            nome_fila.endswith(f"_{self.nome_usuario}")):

                        # Extrair nome do tópico
                        partes = nome_fila.split("_")
                        if len(partes) >= 3:
                            nome_topico = "_".join(partes[1:-1])  # Tudo entre topic_ e _usuario
                            self.topicos_assinados.add(nome_topico)

        except Exception as e:
            print(f"Erro ao carregar assinaturas: {e}")

    # ====== MÉTODOS DE CONSULTA ======

    def buscar_usuarios_disponiveis(self) -> List[str]:
        """
        Retorna lista de usuários disponíveis no sistema

        Returns:
            List[str]: Lista de nomes de usuários
        """
        usuarios = []
        try:
            url = f"http://{ConfiguracaoRabbitMQ.HOST}:{ConfiguracaoRabbitMQ.MANAGEMENT_PORT}/api/queues"
            response = requests.get(
                url,
                auth=HTTPBasicAuth(ConfiguracaoRabbitMQ.USERNAME, ConfiguracaoRabbitMQ.PASSWORD),
                timeout=5
            )

            if response.status_code == 200:
                filas = response.json()
                for fila in filas:
                    nome_fila = fila['name']
                    if nome_fila.startswith("user_"):
                        usuario = nome_fila.replace("user_", "")
                        usuarios.append(usuario)

        except Exception as e:
            print(f"Erro ao buscar usuários: {e}")

        return sorted(usuarios)

    def buscar_topicos_disponiveis(self) -> List[str]:
        """
        Retorna lista de tópicos disponíveis no sistema

        Returns:
            List[str]: Lista de nomes de tópicos
        """
        topicos = []
        try:
            url = f"http://{ConfiguracaoRabbitMQ.HOST}:{ConfiguracaoRabbitMQ.MANAGEMENT_PORT}/api/exchanges"
            response = requests.get(
                url,
                auth=HTTPBasicAuth(ConfiguracaoRabbitMQ.USERNAME, ConfiguracaoRabbitMQ.PASSWORD),
                timeout=5
            )

            if response.status_code == 200:
                exchanges = response.json()
                for exchange in exchanges:
                    # Filtrar apenas exchanges fanout que não são do sistema
                    if (exchange['type'] == 'fanout' and
                            not exchange['name'].startswith("amq.")):
                        topicos.append(exchange['name'])

        except Exception as e:
            print(f"Erro ao buscar tópicos: {e}")

        return sorted(topicos)

    def buscar_filas_gerais(self) -> List[str]:
        """
        Retorna lista de filas gerais (que não são de usuários nem tópicos)

        Returns:
            List[str]: Lista de nomes de filas gerais
        """
        filas = []
        try:
            url = f"http://{ConfiguracaoRabbitMQ.HOST}:{ConfiguracaoRabbitMQ.MANAGEMENT_PORT}/api/queues"
            response = requests.get(
                url,
                auth=HTTPBasicAuth(ConfiguracaoRabbitMQ.USERNAME, ConfiguracaoRabbitMQ.PASSWORD),
                timeout=5
            )

            if response.status_code == 200:
                for fila in response.json():
                    nome_fila = fila['name']
                    # Filtrar filas do sistema, usuários e tópicos
                    if (not nome_fila.startswith("user_") and
                            not nome_fila.startswith("topic_") and
                            not nome_fila.startswith("amq.")):
                        filas.append(nome_fila)

        except Exception as e:
            print(f"Erro ao buscar filas gerais: {e}")

        return sorted(filas)

    # ====== MÉTODOS DE ASSINATURA DE TÓPICOS ======

    def assinar_topico(self, nome_topico: str) -> Tuple[bool, str]:
        """
        Inscreve o usuário em um tópico

        Args:
            nome_topico: Nome do tópico a assinar

        Returns:
            Tuple[bool, str]: (sucesso, mensagem)
        """
        try:
            if not self.esta_conectado():
                return False, "Não conectado ao RabbitMQ"

            # Garantir que o exchange existe
            self.channel.exchange_declare(
                exchange=nome_topico,
                exchange_type='fanout',
                durable=True
            )

            # Criar fila específica para o usuário no tópico
            fila_topico = f"topic_{nome_topico}_{self.nome_usuario}"
            self.channel.queue_declare(queue=fila_topico, durable=True)

            # Vincular fila ao exchange
            self.channel.queue_bind(exchange=nome_topico, queue=fila_topico)

            # Adicionar aos tópicos assinados
            self.topicos_assinados.add(nome_topico)

            return True, f"Inscrito no tópico '{nome_topico}'"

        except Exception as e:
            return False, f"Erro ao assinar tópico: {e}"

    def desassinar_topico(self, nome_topico: str) -> Tuple[bool, str]:
        """
        Remove inscrição do usuário de um tópico

        Args:
            nome_topico: Nome do tópico a desassinar

        Returns:
            Tuple[bool, str]: (sucesso, mensagem)
        """
        try:
            if not self.esta_conectado():
                return False, "Não conectado ao RabbitMQ"

            # Remover fila do tópico
            fila_topico = f"topic_{nome_topico}_{self.nome_usuario}"
            self.channel.queue_delete(queue=fila_topico)

            # Remover dos tópicos assinados
            self.topicos_assinados.discard(nome_topico)

            return True, f"Assinatura do tópico '{nome_topico}' removida"

        except Exception as e:
            return False, f"Erro ao desassinar tópico: {e}"

    # ====== MÉTODOS DE ENVIO DE MENSAGENS ======

    def enviar_mensagem_usuario(self, destinatario: str, conteudo: str) -> Tuple[bool, str]:
        """
        Envia mensagem direta para outro usuário

        Args:
            destinatario: Nome do usuário destinatário
            conteudo: Conteúdo da mensagem

        Returns:
            Tuple[bool, str]: (sucesso, mensagem de status)
        """
        try:
            if not self.esta_conectado():
                return False, "Não conectado ao RabbitMQ"

            # Validar se destinatário existe
            fila_destinatario = f"user_{destinatario}"
            usuarios_disponiveis = self.buscar_usuarios_disponiveis()

            if destinatario not in usuarios_disponiveis:
                return False, f"Usuário '{destinatario}' não existe!"

            # Criar mensagem estruturada
            mensagem = {
                'tipo': TipoMensagem.MENSAGEM_DIRETA,
                'remetente': self.nome_usuario,
                'destinatario': destinatario,
                'conteudo': conteudo,
                'timestamp': datetime.now().isoformat()
            }

            # Enviar mensagem
            self.channel.basic_publish(
                exchange='',  # Exchange padrão (direct)
                routing_key=fila_destinatario,
                body=json.dumps(mensagem, ensure_ascii=False),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mensagem persistente
                    content_type='application/json'
                )
            )

            return True, "Mensagem enviada com sucesso"

        except Exception as e:
            return False, f"Erro ao enviar mensagem: {e}"

    def enviar_mensagem_topico(self, nome_topico: str, conteudo: str) -> Tuple[bool, str]:
        """
        Publica mensagem em um tópico

        Args:
            nome_topico: Nome do tópico
            conteudo: Conteúdo da mensagem

        Returns:
            Tuple[bool, str]: (sucesso, mensagem de status)
        """
        try:
            if not self.esta_conectado():
                return False, "Não conectado ao RabbitMQ"

            # Verificar se tópico existe
            try:
                self.channel.exchange_declare(
                    exchange=nome_topico,
                    exchange_type='fanout',
                    passive=True  # Apenas verificar existência
                )
            except Exception:
                return False, f"Tópico '{nome_topico}' não existe"

            # Criar mensagem estruturada
            mensagem = {
                'tipo': TipoMensagem.MENSAGEM_TOPICO,
                'topico': nome_topico,
                'remetente': self.nome_usuario,
                'conteudo': conteudo,
                'timestamp': datetime.now().isoformat()
            }

            # Publicar mensagem
            self.channel.basic_publish(
                exchange=nome_topico,
                routing_key='',  # Fanout ignora routing key
                body=json.dumps(mensagem, ensure_ascii=False),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mensagem persistente
                    content_type='application/json'
                )
            )

            return True, f"Mensagem publicada no tópico '{nome_topico}'"

        except Exception as e:
            return False, f"Erro ao enviar mensagem para tópico: {e}"

    def enviar_mensagem_fila(self, nome_fila: str, conteudo: str) -> Tuple[bool, str]:
        """
        Envia mensagem para uma fila geral

        Args:
            nome_fila: Nome da fila
            conteudo: Conteúdo da mensagem

        Returns:
            Tuple[bool, str]: (sucesso, mensagem de status)
        """
        try:
            if not self.esta_conectado():
                return False, "Não conectado ao RabbitMQ"

            # Criar mensagem estruturada
            mensagem = {
                'tipo': TipoMensagem.MENSAGEM_FILA,
                'fila': nome_fila,
                'remetente': self.nome_usuario,
                'conteudo': conteudo,
                'timestamp': datetime.now().isoformat()
            }

            # Enviar mensagem
            self.channel.basic_publish(
                exchange='',  # Exchange padrão
                routing_key=nome_fila,
                body=json.dumps(mensagem, ensure_ascii=False),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mensagem persistente
                    content_type='application/json'
                )
            )

            return True, f"Mensagem enviada para a fila '{nome_fila}'"

        except Exception as e:
            return False, f"Erro ao enviar mensagem para fila: {e}"

    # ====== MÉTODOS DE CONSUMO DE MENSAGENS ======

    def iniciar_consumo(self, callback_mensagem: Callable[[Dict], None]) -> None:
        """
        Inicia o consumo assíncrono de mensagens

        Args:
            callback_mensagem: Função de callback para processar mensagens recebidas
        """
        if not self.esta_conectado():
            return

        self.callback_mensagem = callback_mensagem
        self._consuming = True

        # Thread para consumir fila pessoal
        thread_pessoal = threading.Thread(
            target=self._consumir_fila_pessoal,
            daemon=True,
            name=f"Consumer-{self.nome_usuario}-personal"
        )
        thread_pessoal.start()
        self._threads_consumo.append(thread_pessoal)

        # Thread para consumir tópicos
        thread_topicos = threading.Thread(
            target=self._consumir_topicos,
            daemon=True,
            name=f"Consumer-{self.nome_usuario}-topics"
        )
        thread_topicos.start()
        self._threads_consumo.append(thread_topicos)

    def _consumir_fila_pessoal(self) -> None:
        """Thread para consumir mensagens da fila pessoal do usuário"""
        try:
            # Criar conexão separada para consumo
            consumer_connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=ConfiguracaoRabbitMQ.HOST,
                    port=ConfiguracaoRabbitMQ.PORT,
                    virtual_host=ConfiguracaoRabbitMQ.VIRTUAL_HOST
                )
            )
            consumer_channel = consumer_connection.channel()

            def callback_fila_pessoal(ch, method, properties, body):
                """Callback para processar mensagens da fila pessoal"""
                try:
                    mensagem_json = body.decode('utf-8')
                    mensagem = json.loads(mensagem_json)

                    if self.callback_mensagem:
                        self.callback_mensagem(mensagem)

                    # Confirmar processamento da mensagem
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    print(f"Erro ao processar mensagem da fila pessoal: {e}")
                    # Confirmar mesmo com erro para não reprocessar
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            # Configurar consumo
            consumer_channel.basic_qos(prefetch_count=1)
            consumer_channel.basic_consume(
                queue=self.fila_pessoal,
                on_message_callback=callback_fila_pessoal
            )

            # Loop de consumo
            while self._consuming:
                try:
                    consumer_connection.process_data_events(time_limit=1)
                except Exception as e:
                    if self._consuming:  # Só logar se ainda deveria estar consumindo
                        print(f"Erro no consumo da fila pessoal: {e}")
                    break

            consumer_connection.close()

        except Exception as e:
            print(f"Erro na thread de consumo da fila pessoal: {e}")

    def _consumir_topicos(self) -> None:
        """Thread para consumir mensagens dos tópicos assinados"""
        try:
            # Criar conexão separada para consumo de tópicos
            topic_connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=ConfiguracaoRabbitMQ.HOST,
                    port=ConfiguracaoRabbitMQ.PORT,
                    virtual_host=ConfiguracaoRabbitMQ.VIRTUAL_HOST
                )
            )
            topic_channel = topic_connection.channel()

            def callback_topicos(ch, method, properties, body):
                """Callback para processar mensagens de tópicos"""
                try:
                    mensagem_json = body.decode('utf-8')
                    mensagem = json.loads(mensagem_json)

                    if self.callback_mensagem:
                        self.callback_mensagem(mensagem)

                    # Confirmar processamento da mensagem
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    print(f"Erro ao processar mensagem de tópico: {e}")
                    # Confirmar mesmo com erro para não reprocessar
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            # Configurar consumo para cada tópico assinado
            topic_channel.basic_qos(prefetch_count=1)

            for topico in self.topicos_assinados:
                fila_topico = f"topic_{topico}_{self.nome_usuario}"
                try:
                    topic_channel.basic_consume(
                        queue=fila_topico,
                        on_message_callback=callback_topicos
                    )
                except Exception as e:
                    print(f"Erro ao configurar consumo do tópico {topico}: {e}")

            # Loop de consumo
            while self._consuming:
                try:
                    topic_connection.process_data_events(time_limit=1)
                except Exception as e:
                    if self._consuming:  # Só logar se ainda deveria estar consumindo
                        print(f"Erro no consumo de tópicos: {e}")
                    break

            topic_connection.close()

        except Exception as e:
            print(f"Erro na thread de consumo de tópicos: {e}")

    def consumir_uma_mensagem_fila(self, nome_fila: str) -> Tuple[bool, Optional[Dict]]:
        """
        Consome uma única mensagem de uma fila geral

        Args:
            nome_fila: Nome da fila a consumir

        Returns:
            Tuple[bool, Optional[Dict]]: (sucesso, mensagem ou None)
        """
        try:
            if not self.esta_conectado():
                return False, None

            # Tentar consumir uma mensagem
            method_frame, header_frame, body = self.channel.basic_get(
                queue=nome_fila,
                auto_ack=False
            )

            if method_frame:
                try:
                    # Tentar decodificar como JSON
                    mensagem_json = body.decode('utf-8')
                    mensagem = json.loads(mensagem_json)
                except json.JSONDecodeError:
                    # Se não for JSON, criar mensagem simples
                    mensagem = {
                        'tipo': 'mensagem_simples',
                        'conteudo': body.decode('utf-8'),
                        'timestamp': datetime.now().isoformat()
                    }

                # Confirmar recebimento
                self.channel.basic_ack(method_frame.delivery_tag)
                return True, mensagem
            else:
                return False, None  # Fila vazia

        except Exception as e:
            print(f"Erro ao consumir mensagem da fila: {e}")
            return False, None


class UsuarioGUI:
    """
    Interface gráfica do cliente MOM

    Responsabilidades:
    - Criar e gerenciar interface do usuário
    - Coordenar operações entre interface e cliente RabbitMQ
    - Exibir mensagens recebidas
    - Gerenciar assinaturas de tópicos
    """

    def __init__(self, nome_usuario: Optional[str] = None):
        self.cliente = RabbitMQCliente()
        self.root = tk.Tk()
        self._configurar_janela_principal()

        # Estado da interface
        self.mensagens_recebidas: List[Dict] = []
        self.topicos_vars: Dict[str, tk.BooleanVar] = {}

        # Conectar usuário
        if self._conectar_usuario(nome_usuario):
            self._criar_interface()
            self.cliente.iniciar_consumo(self._processar_mensagem_recebida)
        else:
            self.root.destroy()
            return

    def _configurar_janela_principal(self) -> None:
        """Configura as propriedades da janela principal"""
        self.root.title("MOM Cliente - RabbitMQ")
        self.root.geometry("900x700")
        self.root.protocol("WM_DELETE_WINDOW", self._fechar_aplicacao)

        # Centralizar janela
        self.root.update_idletasks()
        x = (self.root.winfo_screenwidth() // 2) - (900 // 2)
        y = (self.root.winfo_screenheight() // 2) - (700 // 2)
        self.root.geometry(f"900x700+{x}+{y}")

    def _conectar_usuario(self, nome_usuario: Optional[str] = None) -> bool:
        """
        Conecta o usuário ao sistema

        Args:
            nome_usuario: Nome do usuário (opcional, se None solicita via dialog)

        Returns:
            bool: True se conexão bem-sucedida
        """
        tentativas = 0
        max_tentativas = 3

        while tentativas < max_tentativas:
            if nome_usuario is None:
                nome = simpledialog.askstring(
                    "Login",
                    "Digite seu nome de usuário:",
                    parent=self.root
                )
            else:
                nome = nome_usuario

            if not nome:
                return False

            nome = nome.strip()

            if nome and self.cliente.conectar(nome):
                self.root.title(f"MOM Cliente - {nome}")
                return True

            # Se nome_usuario foi especificado e falhou, não tentar novamente
            if nome_usuario is not None:
                return False

            tentativas += 1

        return False

    def _criar_interface(self) -> None:
        """Cria e organiza a interface gráfica principal"""
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Cabeçalho
        titulo = ttk.Label(
            main_frame,
            text=f"Cliente MOM RabbitMQ - {self.cliente.nome_usuario}",
            font=('Arial', 16, 'bold')
        )
        titulo.pack(pady=(0, 20))

        # Notebook com abas
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        # Criar abas
        self._criar_aba_mensagens_recebidas()
        self._criar_aba_enviar_usuario()
        self._criar_aba_topicos()
        self._criar_aba_filas_gerais()

        # Barra de status
        self.status_label = ttk.Label(
            main_frame,
            text="Conectado ao RabbitMQ",
            foreground="green",
            font=('Arial', 10)
        )
        self.status_label.pack()

    def _criar_aba_mensagens_recebidas(self) -> None:
        """Cria a aba de exibição de mensagens recebidas"""
        aba_mensagens = ttk.Frame(self.notebook)
        self.notebook.add(aba_mensagens, text="📬 Mensagens Recebidas")

        frame = ttk.Frame(aba_mensagens, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        # Título
        ttk.Label(
            frame,
            text="Mensagens Recebidas",
            font=('Arial', 12, 'bold')
        ).pack(anchor=tk.W, pady=(0, 10))

        # Frame para área de mensagens
        frame_mensagens = ttk.Frame(frame)
        frame_mensagens.pack(fill=tk.BOTH, expand=True)

        # Área de texto com scrollbar
        scrollbar_msg = ttk.Scrollbar(frame_mensagens, orient=tk.VERTICAL)
        scrollbar_msg.pack(side=tk.RIGHT, fill=tk.Y)

        self.text_mensagens = tk.Text(
            frame_mensagens,
            wrap=tk.WORD,
            state=tk.DISABLED,
            font=('Arial', 10),
            yscrollcommand=scrollbar_msg.set
        )
        self.text_mensagens.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar_msg.config(command=self.text_mensagens.yview)

        # Botão para limpar mensagens
        ttk.Button(
            frame,
            text="🗑️ Limpar Mensagens",
            command=self._limpar_mensagens
        ).pack(pady=(10, 0))

    def _criar_aba_enviar_usuario(self) -> None:
        """Cria a aba para envio de mensagens diretas"""
        aba_usuario = ttk.Frame(self.notebook)
        self.notebook.add(aba_usuario, text="👤 Mensagem Direta")

        frame = ttk.Frame(aba_usuario, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        # Título
        ttk.Label(
            frame,
            text="Enviar Mensagem Direta",
            font=('Arial', 12, 'bold')
        ).pack(anchor=tk.W, pady=(0, 20))

        # Seleção de destinatário
        frame_destinatario = ttk.Frame(frame)
        frame_destinatario.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame_destinatario, text="Destinatário:").pack(anchor=tk.W)

        frame_dest_botao = ttk.Frame(frame_destinatario)
        frame_dest_botao.pack(fill=tk.X, pady=(0, 5))

        self.combo_destinatario = ttk.Combobox(
            frame_dest_botao,
            state="readonly",
            font=('Arial', 11)
        )
        self.combo_destinatario.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))

        ttk.Button(
            frame_dest_botao,
            text="🔄 Atualizar",
            command=self._atualizar_lista_usuarios
        ).pack(side=tk.RIGHT)

        # Área de mensagem
        ttk.Label(frame, text="Mensagem:").pack(anchor=tk.W)

        frame_texto_usuario = ttk.Frame(frame)
        frame_texto_usuario.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        scrollbar_usuario = ttk.Scrollbar(frame_texto_usuario, orient=tk.VERTICAL)
        scrollbar_usuario.pack(side=tk.RIGHT, fill=tk.Y)

        self.text_mensagem_usuario = tk.Text(
            frame_texto_usuario,
            height=10,
            wrap=tk.WORD,
            font=('Arial', 10),
            yscrollcommand=scrollbar_usuario.set
        )
        self.text_mensagem_usuario.pack(fill=tk.BOTH, expand=True)
        scrollbar_usuario.config(command=self.text_mensagem_usuario.yview)

        # Botão enviar
        ttk.Button(
            frame,
            text="📤 Enviar Mensagem",
            command=self._enviar_mensagem_usuario
        ).pack()

        # Carregar usuários iniciais
        self._atualizar_lista_usuarios()

    def _criar_aba_topicos(self) -> None:
        """Cria a aba de gerenciamento de tópicos"""
        aba_topicos = ttk.Frame(self.notebook)
        self.notebook.add(aba_topicos, text="📢 Tópicos")

        frame = ttk.Frame(aba_topicos, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        # ---- Seção de Assinaturas ----
        frame_assinaturas = ttk.LabelFrame(frame, text="Gerenciar Assinaturas", padding="10")
        frame_assinaturas.pack(fill=tk.BOTH, expand=True, pady=(0, 20))

        # Frame scrollável para checkboxes
        canvas_assinaturas = tk.Canvas(frame_assinaturas)
        scrollbar_assinaturas = ttk.Scrollbar(
            frame_assinaturas,
            orient="vertical",
            command=canvas_assinaturas.yview
        )

        self.frame_checkboxes = ttk.Frame(canvas_assinaturas)

        canvas_assinaturas.configure(yscrollcommand=scrollbar_assinaturas.set)
        canvas_assinaturas.create_window((0, 0), window=self.frame_checkboxes, anchor="nw")

        canvas_assinaturas.pack(side="left", fill="both", expand=True)
        scrollbar_assinaturas.pack(side="right", fill="y")

        # Atualizar região scrollável
        self.frame_checkboxes.bind(
            "<Configure>",
            lambda e: canvas_assinaturas.configure(scrollregion=canvas_assinaturas.bbox("all"))
        )

        # Botão atualizar assinaturas
        ttk.Button(
            frame_assinaturas,
            text="🔄 Atualizar Tópicos",
            command=self._atualizar_checkboxes_topicos
        ).pack(pady=(10, 0))

        # ---- Seção de Envio ----
        frame_envio = ttk.LabelFrame(frame, text="Publicar Mensagem", padding="10")
        frame_envio.pack(fill=tk.BOTH, expand=True)

        # Seleção de tópico
        frame_topico_sel = ttk.Frame(frame_envio)
        frame_topico_sel.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame_topico_sel, text="Tópico:").pack(anchor=tk.W)

        frame_combo_topicos = ttk.Frame(frame_topico_sel)
        frame_combo_topicos.pack(fill=tk.X, pady=(0, 5))

        self.combo_topicos = ttk.Combobox(
            frame_combo_topicos,
            state="readonly",
            font=('Arial', 11)
        )
        self.combo_topicos.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))

        ttk.Button(
            frame_combo_topicos,
            text="🔄 Atualizar",
            command=self._atualizar_combo_topicos
        ).pack(side=tk.RIGHT)

        # Área de mensagem
        ttk.Label(frame_envio, text="Mensagem:").pack(anchor=tk.W)

        frame_texto_topico = ttk.Frame(frame_envio)
        frame_texto_topico.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        scrollbar_topico = ttk.Scrollbar(frame_texto_topico, orient=tk.VERTICAL)
        scrollbar_topico.pack(side=tk.RIGHT, fill=tk.Y)

        self.text_mensagem_topico = tk.Text(
            frame_texto_topico,
            height=6,
            wrap=tk.WORD,
            font=('Arial', 10),
            yscrollcommand=scrollbar_topico.set
        )
        self.text_mensagem_topico.pack(fill=tk.BOTH, expand=True)
        scrollbar_topico.config(command=self.text_mensagem_topico.yview)

        # Botão publicar
        ttk.Button(
            frame_envio,
            text="📡 Publicar no Tópico",
            command=self._enviar_mensagem_topico
        ).pack()

        # Carregar dados iniciais
        self._atualizar_checkboxes_topicos()
        self._atualizar_combo_topicos()

    def _criar_aba_filas_gerais(self) -> None:
        """Cria a aba de interação com filas gerais"""
        aba_filas = ttk.Frame(self.notebook)
        self.notebook.add(aba_filas, text="📦 Filas Gerais")

        frame = ttk.Frame(aba_filas, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        # Título
        ttk.Label(
            frame,
            text="Enviar e Receber de Filas Gerais",
            font=('Arial', 12, 'bold')
        ).pack(anchor=tk.W, pady=(0, 10))

        # Seleção de fila
        frame_fila_sel = ttk.Frame(frame)
        frame_fila_sel.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame_fila_sel, text="Fila:").pack(anchor=tk.W)

        frame_combo_filas = ttk.Frame(frame_fila_sel)
        frame_combo_filas.pack(fill=tk.X, pady=(0, 5))

        self.combo_filas = ttk.Combobox(
            frame_combo_filas,
            state="readonly",
            font=('Arial', 11)
        )
        self.combo_filas.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))

        ttk.Button(
            frame_combo_filas,
            text="🔄 Atualizar Lista",
            command=self._atualizar_lista_filas
        ).pack(side=tk.RIGHT)

        # Área de mensagem
        ttk.Label(frame, text="Mensagem:").pack(anchor=tk.W)

        frame_texto_fila = ttk.Frame(frame)
        frame_texto_fila.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        scrollbar_fila = ttk.Scrollbar(frame_texto_fila, orient=tk.VERTICAL)
        scrollbar_fila.pack(side=tk.RIGHT, fill=tk.Y)

        self.text_mensagem_fila = tk.Text(
            frame_texto_fila,
            height=6,
            wrap=tk.WORD,
            font=('Arial', 10),
            yscrollcommand=scrollbar_fila.set
        )
        self.text_mensagem_fila.pack(fill=tk.BOTH, expand=True)
        scrollbar_fila.config(command=self.text_mensagem_fila.yview)

        # Botões de ação
        frame_botoes_fila = ttk.Frame(frame)
        frame_botoes_fila.pack(pady=(5, 0))

        ttk.Button(
            frame_botoes_fila,
            text="📤 Enviar para Fila",
            command=self._enviar_mensagem_fila
        ).pack(side=tk.LEFT, padx=(0, 5))

        ttk.Button(
            frame_botoes_fila,
            text="📥 Consumir 1 Mensagem",
            command=self._consumir_uma_mensagem_fila
        ).pack(side=tk.LEFT)

        # Carregar filas iniciais
        self._atualizar_lista_filas()

    # ====== MÉTODOS DE ATUALIZAÇÃO DE LISTAS ======

    def _atualizar_lista_usuarios(self) -> None:
        """Atualiza a lista de usuários disponíveis"""
        try:
            usuarios = self.cliente.buscar_usuarios_disponiveis()

            # Remover o próprio usuário da lista
            if self.cliente.nome_usuario in usuarios:
                usuarios.remove(self.cliente.nome_usuario)

            self.combo_destinatario['values'] = usuarios

            # Limpar seleção se usuário atual não existe mais
            if self.combo_destinatario.get() not in usuarios:
                self.combo_destinatario.set('')

        except Exception as e:
            print(f"Erro ao atualizar lista de usuários: {e}")
            messagebox.showerror("Erro", f"Erro ao atualizar usuários: {e}")

    def _atualizar_checkboxes_topicos(self) -> None:
        """Atualiza os checkboxes de assinatura de tópicos"""
        try:
            # Limpar checkboxes existentes
            for widget in self.frame_checkboxes.winfo_children():
                widget.destroy()
            self.topicos_vars.clear()

            # Buscar tópicos disponíveis
            topicos_disponiveis = self.cliente.buscar_topicos_disponiveis()

            if not topicos_disponiveis:
                ttk.Label(
                    self.frame_checkboxes,
                    text="Nenhum tópico disponível",
                    font=('Arial', 10, 'italic')
                ).pack(anchor=tk.W, pady=10)
                return

            # Criar checkboxes para cada tópico
            for topico in sorted(topicos_disponiveis):
                inscrito = topico in self.cliente.topicos_assinados

                var = tk.BooleanVar(value=inscrito)

                checkbox = ttk.Checkbutton(
                    self.frame_checkboxes,
                    text=f"📢 {topico}",
                    variable=var,
                    command=lambda t=topico, v=var: self._toggle_assinatura_topico(t, v)
                )
                checkbox.pack(anchor=tk.W, pady=2)

                self.topicos_vars[topico] = var

        except Exception as e:
            print(f"Erro ao atualizar checkboxes de tópicos: {e}")
            messagebox.showerror("Erro", f"Erro ao atualizar tópicos: {e}")

    def _atualizar_combo_topicos(self) -> None:
        """Atualiza o combobox de tópicos para envio"""
        try:
            topicos = self.cliente.buscar_topicos_disponiveis()
            self.combo_topicos['values'] = topicos

            # Limpar seleção se tópico atual não existe mais
            if self.combo_topicos.get() not in topicos:
                self.combo_topicos.set('')

        except Exception as e:
            print(f"Erro ao atualizar combo de tópicos: {e}")

    def _atualizar_lista_filas(self) -> None:
        """Atualiza a lista de filas gerais"""
        try:
            filas = self.cliente.buscar_filas_gerais()
            self.combo_filas['values'] = filas

            # Selecionar primeira fila se disponível e nenhuma selecionada
            if filas and not self.combo_filas.get():
                self.combo_filas.current(0)

        except Exception as e:
            print(f"Erro ao atualizar lista de filas: {e}")
            messagebox.showerror("Erro", f"Erro ao atualizar filas: {e}")

    # ====== MÉTODOS DE ENVIO DE MENSAGENS ======

    def _enviar_mensagem_usuario(self) -> None:
        """Envia mensagem direta para outro usuário"""
        destinatario = self.combo_destinatario.get().strip()
        conteudo = self.text_mensagem_usuario.get('1.0', tk.END).strip()

        # Validações
        if not destinatario:
            messagebox.showwarning("Aviso", "Selecione um destinatário!")
            return

        if not conteudo:
            messagebox.showwarning("Aviso", "Digite uma mensagem!")
            return

        if len(conteudo) > 5000:  # Limite de tamanho
            messagebox.showwarning("Aviso", "Mensagem muito longa! Máximo 5000 caracteres.")
            return

        # Enviar mensagem
        sucesso, mensagem_status = self.cliente.enviar_mensagem_usuario(destinatario, conteudo)

        if sucesso:
            messagebox.showinfo("Sucesso", "Mensagem enviada com sucesso!")
            # Limpar campos
            self.text_mensagem_usuario.delete('1.0', tk.END)
            self.combo_destinatario.set('')
        else:
            messagebox.showerror("Erro", mensagem_status)

    def _enviar_mensagem_topico(self) -> None:
        """Publica mensagem em um tópico"""
        topico = self.combo_topicos.get().strip()
        conteudo = self.text_mensagem_topico.get('1.0', tk.END).strip()

        # Validações
        if not topico:
            messagebox.showwarning("Aviso", "Selecione um tópico!")
            return

        if not conteudo:
            messagebox.showwarning("Aviso", "Digite uma mensagem!")
            return

        if len(conteudo) > 5000:  # Limite de tamanho
            messagebox.showwarning("Aviso", "Mensagem muito longa! Máximo 5000 caracteres.")
            return

        # Enviar mensagem
        sucesso, mensagem_status = self.cliente.enviar_mensagem_topico(topico, conteudo)

        if sucesso:
            messagebox.showinfo("Sucesso", mensagem_status)
            # Limpar campo de mensagem
            self.text_mensagem_topico.delete('1.0', tk.END)
        else:
            messagebox.showerror("Erro", mensagem_status)

    def _enviar_mensagem_fila(self) -> None:
        """Envia mensagem para uma fila geral"""
        fila = self.combo_filas.get().strip()
        conteudo = self.text_mensagem_fila.get('1.0', tk.END).strip()

        # Validações
        if not fila:
            messagebox.showwarning("Aviso", "Selecione uma fila!")
            return

        if not conteudo:
            messagebox.showwarning("Aviso", "Digite uma mensagem!")
            return

        if len(conteudo) > 5000:  # Limite de tamanho
            messagebox.showwarning("Aviso", "Mensagem muito longa! Máximo 5000 caracteres.")
            return

        # Enviar mensagem
        sucesso, mensagem_status = self.cliente.enviar_mensagem_fila(fila, conteudo)

        if sucesso:
            messagebox.showinfo("Sucesso", mensagem_status)
            # Limpar campo de mensagem
            self.text_mensagem_fila.delete('1.0', tk.END)
        else:
            messagebox.showerror("Erro", mensagem_status)

    # ====== MÉTODOS DE CONSUMO E EXIBIÇÃO ======

    def _consumir_uma_mensagem_fila(self) -> None:
        """Consome uma única mensagem de uma fila geral"""
        fila = self.combo_filas.get().strip()

        if not fila:
            messagebox.showwarning("Aviso", "Selecione uma fila!")
            return

        sucesso, mensagem = self.cliente.consumir_uma_mensagem_fila(fila)

        if sucesso and mensagem:
            self._processar_mensagem_recebida(mensagem)
            messagebox.showinfo("Sucesso", f"Mensagem consumida da fila '{fila}'")
        elif not sucesso:
            messagebox.showerror("Erro", "Falha ao consumir mensagem")
        else:
            messagebox.showinfo("Fila Vazia", f"Não há mensagens na fila '{fila}'")

    def _toggle_assinatura_topico(self, nome_topico: str, var: tk.BooleanVar) -> None:
        """Alterna assinatura de um tópico"""
        try:
            if var.get():
                # Assinar tópico
                sucesso, mensagem = self.cliente.assinar_topico(nome_topico)
                if sucesso:
                    messagebox.showinfo("Sucesso", mensagem)
                    # Reinicializar consumo para incluir novo tópico
                    self._reiniciar_consumo()
                else:
                    # Reverter checkbox em caso de erro
                    var.set(False)
                    messagebox.showerror("Erro", mensagem)
            else:
                # Desassinar tópico
                sucesso, mensagem = self.cliente.desassinar_topico(nome_topico)
                if sucesso:
                    messagebox.showinfo("Sucesso", mensagem)
                else:
                    # Reverter checkbox em caso de erro
                    var.set(True)
                    messagebox.showerror("Erro", mensagem)

        except Exception as e:
            # Reverter checkbox em caso de exceção
            var.set(not var.get())
            messagebox.showerror("Erro", f"Erro ao alterar assinatura: {e}")

    def _reiniciar_consumo(self) -> None:
        """Reinicia o consumo de mensagens para incluir novos tópicos"""
        try:
            # Parar consumo atual
            self.cliente._consuming = False

            # Aguardar threads terminarem
            time.sleep(1)

            # Reiniciar consumo
            self.cliente.iniciar_consumo(self._processar_mensagem_recebida)

        except Exception as e:
            print(f"Erro ao reiniciar consumo: {e}")

    def _processar_mensagem_recebida(self, mensagem: Dict) -> None:
        """
        Processa mensagem recebida e agenda atualização da GUI

        Args:
            mensagem: Dicionário com dados da mensagem
        """

        def atualizar_gui():
            """Função para atualizar GUI na thread principal"""
            try:
                self.mensagens_recebidas.append(mensagem)
                self._exibir_mensagem(mensagem)
            except Exception as e:
                print(f"Erro ao atualizar GUI: {e}")

        # Agendar atualização na thread principal
        self.root.after(0, atualizar_gui)

    def _exibir_mensagem(self, mensagem: Dict) -> None:
        """
        Exibe mensagem na área de mensagens recebidas

        Args:
            mensagem: Dicionário com dados da mensagem
        """
        try:
            self.text_mensagens.config(state=tk.NORMAL)

            # Formatar timestamp
            try:
                timestamp_str = mensagem.get('timestamp', '')
                if timestamp_str:
                    timestamp = datetime.fromisoformat(timestamp_str).strftime('%H:%M:%S')
                else:
                    timestamp = datetime.now().strftime('%H:%M:%S')
            except Exception:
                timestamp = datetime.now().strftime('%H:%M:%S')

            # Processar por tipo de mensagem
            tipo = mensagem.get('tipo', 'desconhecido')

            if tipo == TipoMensagem.MENSAGEM_TOPICO:
                self._exibir_mensagem_topico(mensagem, timestamp)
            elif tipo == TipoMensagem.MENSAGEM_DIRETA:
                self._exibir_mensagem_direta(mensagem, timestamp)
            elif tipo == TipoMensagem.MENSAGEM_FILA:
                self._exibir_mensagem_fila(mensagem, timestamp)
            else:
                self._exibir_mensagem_generica(mensagem, timestamp)

            # Rolar para o final
            self.text_mensagens.config(state=tk.DISABLED)
            self.text_mensagens.see(tk.END)

        except Exception as e:
            print(f"Erro ao exibir mensagem: {e}")

    def _exibir_mensagem_topico(self, mensagem: Dict, timestamp: str) -> None:
        """Exibe mensagem de tópico formatada"""
        topico = mensagem.get('topico', 'Desconhecido')
        remetente = mensagem.get('remetente', 'Desconhecido')
        conteudo = mensagem.get('conteudo', '')

        self.text_mensagens.insert(tk.END, f"[{timestamp}] 📢 TÓPICO '{topico}' - {remetente}:\n")
        self.text_mensagens.insert(tk.END, f"{conteudo}\n")
        self.text_mensagens.insert(tk.END, "=" * 60 + "\n\n")

    def _exibir_mensagem_direta(self, mensagem: Dict, timestamp: str) -> None:
        """Exibe mensagem direta formatada"""
        remetente = mensagem.get('remetente', 'Desconhecido')
        conteudo = mensagem.get('conteudo', '')

        self.text_mensagens.insert(tk.END, f"[{timestamp}] 💬 {remetente}:\n")
        self.text_mensagens.insert(tk.END, f"{conteudo}\n")
        self.text_mensagens.insert(tk.END, "-" * 50 + "\n\n")

    def _exibir_mensagem_fila(self, mensagem: Dict, timestamp: str) -> None:
        """Exibe mensagem de fila formatada"""
        fila = mensagem.get('fila', 'Desconhecida')
        remetente = mensagem.get('remetente', 'Desconhecido')
        conteudo = mensagem.get('conteudo', '')

        self.text_mensagens.insert(tk.END, f"[{timestamp}] 📦 FILA '{fila}' - {remetente}:\n")
        self.text_mensagens.insert(tk.END, f"{conteudo}\n")
        self.text_mensagens.insert(tk.END, "#" * 60 + "\n\n")

    def _exibir_mensagem_generica(self, mensagem: Dict, timestamp: str) -> None:
        """Exibe mensagem genérica/desconhecida"""
        conteudo = mensagem.get('conteudo', str(mensagem))

        self.text_mensagens.insert(tk.END, f"[{timestamp}] ❓ MENSAGEM:\n")
        self.text_mensagens.insert(tk.END, f"{conteudo}\n")
        self.text_mensagens.insert(tk.END, "~" * 40 + "\n\n")

    def _limpar_mensagens(self) -> None:
        """Limpa a área de mensagens recebidas"""
        if messagebox.askyesno("Confirmação", "Deseja limpar todas as mensagens?"):
            self.text_mensagens.config(state=tk.NORMAL)
            self.text_mensagens.delete('1.0', tk.END)
            self.text_mensagens.config(state=tk.DISABLED)
            self.mensagens_recebidas.clear()

    def _fechar_aplicacao(self) -> None:
        """Fecha a aplicação de forma segura"""
        try:
            self.cliente.desconectar()
        except Exception as e:
            print(f"Erro ao fechar aplicação: {e}")
        finally:
            self.root.destroy()

    def executar(self) -> None:
        """Inicia o loop principal da aplicação"""
        if self.cliente.esta_conectado():
            try:
                self.root.mainloop()
            except KeyboardInterrupt:
                print("Aplicação fechada pelo usuário")
            except Exception as e:
                print(f"Erro na execução: {e}")
            finally:
                self._fechar_aplicacao()


def main():
    """Função principal - ponto de entrada da aplicação"""
    try:
        # Verificar se nome de usuário foi passado como argumento
        nome_usuario = sys.argv[1] if len(sys.argv) > 1 else None

        # Criar e executar aplicação
        app = UsuarioGUI(nome_usuario)
        app.executar()

    except Exception as e:
        print(f"Erro fatal na inicialização do cliente: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()