import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import pika
import json
import threading
import time
from datetime import datetime
import uuid


class RabbitMQCliente:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.conectado = False
        self.nome_usuario = ""
        self.fila_pessoal = ""
        self.callback_mensagem = None
        self.topicos_assinados = set()
        self.consuming = False

    def conectar(self, nome_usuario):
        """Conecta ao RabbitMQ e configura o usuário"""
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters('localhost')
            )
            self.channel = self.connection.channel()

            self.nome_usuario = nome_usuario
            self.fila_pessoal = f"user_{nome_usuario}"

            # Declarar fila pessoal
            self.channel.queue_declare(queue=self.fila_pessoal, durable=True)

            self.conectado = True
            return True

        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao conectar ao RabbitMQ: {e}")
            return False

    def desconectar(self):
        """Desconecta do RabbitMQ"""
        self.consuming = False
        if self.connection and not self.connection.is_closed:
            try:
                self.connection.close()
            except:
                pass
        self.conectado = False

    def assinar_topico(self, nome_topico):
        """Assina um tópico (exchange)"""
        try:
            if not self.conectado:
                return False, "Não conectado ao RabbitMQ"

            # Declarar exchange se não existir
            self.channel.exchange_declare(
                exchange=nome_topico,
                exchange_type='fanout',
                durable=True
            )

            # Criar fila temporária para o tópico ou usar fila exclusiva
            fila_topico = f"topic_{nome_topico}_{self.nome_usuario}"
            self.channel.queue_declare(queue=fila_topico, durable=True)

            # Fazer bind da fila com o exchange
            self.channel.queue_bind(
                exchange=nome_topico,
                queue=fila_topico
            )

            self.topicos_assinados.add(nome_topico)
            return True, f"Inscrito no tópico '{nome_topico}'"

        except Exception as e:
            return False, f"Erro ao assinar tópico: {e}"

    def enviar_mensagem_usuario(self, destinatario, conteudo):
        """Envia mensagem para outro usuário"""
        try:
            if not self.conectado:
                return False, "Não conectado ao RabbitMQ"

            fila_destinatario = f"user_{destinatario}"

            # Declarar fila do destinatário (caso não exista)
            self.channel.queue_declare(queue=fila_destinatario, durable=True)

            mensagem = {
                'tipo': 'mensagem_direta',
                'remetente': self.nome_usuario,
                'destinatario': destinatario,
                'conteudo': conteudo,
                'timestamp': datetime.now().isoformat()
            }

            self.channel.basic_publish(
                exchange='',
                routing_key=fila_destinatario,
                body=json.dumps(mensagem),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Tornar mensagem persistente
                )
            )

            return True, "Mensagem enviada"

        except Exception as e:
            return False, f"Erro ao enviar mensagem: {e}"

    def enviar_mensagem_topico(self, nome_topico, conteudo):
        """Envia mensagem para um tópico"""
        try:
            if not self.conectado:
                return False, "Não conectado ao RabbitMQ"

            # Verificar se exchange existe
            try:
                self.channel.exchange_declare(
                    exchange=nome_topico,
                    exchange_type='fanout',
                    passive=True
                )
            except:
                return False, f"Tópico '{nome_topico}' não existe"

            mensagem = {
                'tipo': 'mensagem_topico',
                'topico': nome_topico,
                'remetente': self.nome_usuario,
                'conteudo': conteudo,
                'timestamp': datetime.now().isoformat()
            }

            self.channel.basic_publish(
                exchange=nome_topico,
                routing_key='',
                body=json.dumps(mensagem),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Tornar mensagem persistente
                )
            )

            return True, f"Mensagem enviada para o tópico '{nome_topico}'"

        except Exception as e:
            return False, f"Erro ao enviar mensagem para tópico: {e}"

    def iniciar_consumo(self, callback_mensagem):
        """Inicia o consumo de mensagens"""
        if not self.conectado:
            return

        self.callback_mensagem = callback_mensagem
        self.consuming = True

        # Thread para consumir mensagens da fila pessoal
        threading.Thread(target=self._consumir_fila_pessoal, daemon=True).start()

        # Thread para consumir mensagens dos tópicos
        threading.Thread(target=self._consumir_topicos, daemon=True).start()

    def _consumir_fila_pessoal(self):
        """Consome mensagens da fila pessoal"""
        try:
            # Criar nova conexão para o consumer
            consumer_connection = pika.BlockingConnection(
                pika.ConnectionParameters('localhost')
            )
            consumer_channel = consumer_connection.channel()

            def callback(ch, method, properties, body):
                try:
                    mensagem = json.loads(body.decode('utf-8'))
                    if self.callback_mensagem:
                        self.callback_mensagem(mensagem)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    print(f"Erro ao processar mensagem: {e}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            consumer_channel.basic_qos(prefetch_count=1)
            consumer_channel.basic_consume(
                queue=self.fila_pessoal,
                on_message_callback=callback
            )

            while self.consuming:
                try:
                    consumer_connection.process_data_events(time_limit=1)
                except:
                    break

            consumer_connection.close()

        except Exception as e:
            print(f"Erro no consumo da fila pessoal: {e}")

    def _consumir_topicos(self):
        """Consome mensagens dos tópicos assinados"""
        try:
            # Criar nova conexão para o consumer de tópicos
            topic_connection = pika.BlockingConnection(
                pika.ConnectionParameters('localhost')
            )
            topic_channel = topic_connection.channel()

            def callback(ch, method, properties, body):
                try:
                    mensagem = json.loads(body.decode('utf-8'))
                    if self.callback_mensagem:
                        self.callback_mensagem(mensagem)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    print(f"Erro ao processar mensagem de tópico: {e}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            # Configurar consumo para cada tópico assinado
            for topico in self.topicos_assinados:
                fila_topico = f"topic_{topico}_{self.nome_usuario}"
                topic_channel.basic_consume(
                    queue=fila_topico,
                    on_message_callback=callback
                )

            while self.consuming:
                try:
                    topic_connection.process_data_events(time_limit=1)

                    # Verificar se há novos tópicos para assinar
                    for topico in self.topicos_assinados:
                        fila_topico = f"topic_{topico}_{self.nome_usuario}"
                        try:
                            topic_channel.basic_consume(
                                queue=fila_topico,
                                on_message_callback=callback
                            )
                        except:
                            pass  # Já está sendo consumido
                except:
                    break

            topic_connection.close()

        except Exception as e:
            print(f"Erro no consumo de tópicos: {e}")


class UsuarioGUI:
    def __init__(self):
        self.cliente = RabbitMQCliente()
        self.root = tk.Tk()
        self.root.title("MOM Cliente - RabbitMQ")
        self.root.geometry("900x700")
        self.root.protocol("WM_DELETE_WINDOW", self.fechar_aplicacao)

        self.mensagens_recebidas = []
        self.conectar_usuario()

        if self.cliente.conectado:
            self.criar_interface()
            self.cliente.iniciar_consumo(self.processar_mensagem_recebida)
        else:
            self.root.destroy()
            return

    def conectar_usuario(self):
        """Solicita nome do usuário e conecta"""
        while True:
            nome = simpledialog.askstring("Login", "Digite seu nome de usuário:",
                                          parent=self.root)
            if not nome:
                self.root.destroy()
                return

            nome = nome.strip()
            if nome and self.cliente.conectar(nome):
                self.root.title(f"MOM Cliente - {nome}")
                break

    def criar_interface(self):
        """Cria a interface gráfica do cliente"""
        # Frame principal
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Título
        titulo = ttk.Label(main_frame, text=f"Cliente MOM RabbitMQ - {self.cliente.nome_usuario}",
                           font=('Arial', 16, 'bold'))
        titulo.pack(pady=(0, 20))

        # Notebook para abas
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        # Aba Mensagens
        frame_mensagens = ttk.Frame(notebook)
        notebook.add(frame_mensagens, text="Mensagens Recebidas")
        self.criar_aba_mensagens(frame_mensagens)

        # Aba Enviar para Usuário
        frame_usuario = ttk.Frame(notebook)
        notebook.add(frame_usuario, text="Enviar para Usuário")
        self.criar_aba_enviar_usuario(frame_usuario)

        # Aba Tópicos
        frame_topicos = ttk.Frame(notebook)
        notebook.add(frame_topicos, text="Tópicos")
        self.criar_aba_topicos(frame_topicos)

        # Status
        self.status_label = ttk.Label(main_frame, text="Conectado ao RabbitMQ",
                                      foreground="green", font=('Arial', 10))
        self.status_label.pack()

    def criar_aba_mensagens(self, parent):
        """Cria a aba de visualização de mensagens"""
        frame = ttk.Frame(parent, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        # Título
        ttk.Label(frame, text="Mensagens Recebidas",
                  font=('Arial', 12, 'bold')).pack(anchor=tk.W, pady=(0, 10))

        # Área de mensagens
        frame_mensagens = ttk.Frame(frame)
        frame_mensagens.pack(fill=tk.BOTH, expand=True)

        # Text widget com scrollbar
        self.text_mensagens = tk.Text(frame_mensagens, wrap=tk.WORD, state=tk.DISABLED,
                                      font=('Arial', 10))
        scrollbar_msg = ttk.Scrollbar(frame_mensagens, orient=tk.VERTICAL,
                                      command=self.text_mensagens.yview)
        self.text_mensagens.configure(yscrollcommand=scrollbar_msg.set)

        self.text_mensagens.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar_msg.pack(side=tk.RIGHT, fill=tk.Y)

        # Botão limpar
        ttk.Button(frame, text="Limpar Mensagens",
                   command=self.limpar_mensagens).pack(pady=(10, 0))

    def criar_aba_enviar_usuario(self, parent):
        """Cria a aba para enviar mensagens para usuários"""
        frame = ttk.Frame(parent, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        # Título
        ttk.Label(frame, text="Enviar Mensagem para Usuário",
                  font=('Arial', 12, 'bold')).pack(anchor=tk.W, pady=(0, 20))

        # Campo destinatário
        ttk.Label(frame, text="Destinatário:").pack(anchor=tk.W)
        self.entry_destinatario = ttk.Entry(frame, width=50, font=('Arial', 11))
        self.entry_destinatario.pack(fill=tk.X, pady=(0, 10))

        # Campo mensagem
        ttk.Label(frame, text="Mensagem:").pack(anchor=tk.W)
        self.text_mensagem_usuario = tk.Text(frame, height=10, wrap=tk.WORD, font=('Arial', 10))
        self.text_mensagem_usuario.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        # Botão enviar
        ttk.Button(frame, text="Enviar Mensagem",
                   command=self.enviar_mensagem_usuario).pack()

    def criar_aba_topicos(self, parent):
        """Cria a aba de gerenciamento de tópicos"""
        frame = ttk.Frame(parent, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        # Seção Assinar Tópico
        frame_assinar = ttk.LabelFrame(frame, text="Assinar Tópico", padding="10")
        frame_assinar.pack(fill=tk.X, pady=(0, 20))

        ttk.Label(frame_assinar, text="Nome do Tópico:").pack(anchor=tk.W)
        self.entry_topico_assinar = ttk.Entry(frame_assinar, width=50, font=('Arial', 11))
        self.entry_topico_assinar.pack(fill=tk.X, pady=(0, 10))

        ttk.Button(frame_assinar, text="Assinar Tópico",
                   command=self.assinar_topico).pack()

        # Lista de tópicos assinados
        ttk.Label(frame_assinar, text="Tópicos Assinados:",
                  font=('Arial', 10, 'bold')).pack(anchor=tk.W, pady=(10, 5))

        self.listbox_topicos = tk.Listbox(frame_assinar, height=3, font=('Arial', 9))
        self.listbox_topicos.pack(fill=tk.X, pady=(0, 5))

        # Seção Enviar para Tópico
        frame_enviar_topico = ttk.LabelFrame(frame, text="Enviar Mensagem para Tópico", padding="10")
        frame_enviar_topico.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame_enviar_topico, text="Tópico:").pack(anchor=tk.W)
        self.entry_topico_enviar = ttk.Entry(frame_enviar_topico, width=50, font=('Arial', 11))
        self.entry_topico_enviar.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame_enviar_topico, text="Mensagem:").pack(anchor=tk.W)
        self.text_mensagem_topico = tk.Text(frame_enviar_topico, height=8, wrap=tk.WORD, font=('Arial', 10))
        self.text_mensagem_topico.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        ttk.Button(frame_enviar_topico, text="Enviar para Tópico",
                   command=self.enviar_mensagem_topico).pack()

    def enviar_mensagem_usuario(self):
        """Envia mensagem para outro usuário"""
        destinatario = self.entry_destinatario.get().strip()
        conteudo = self.text_mensagem_usuario.get('1.0', tk.END).strip()

        if not destinatario:
            messagebox.showwarning("Aviso", "Digite o nome do destinatário!")
            return

        if not conteudo:
            messagebox.showwarning("Aviso", "Digite uma mensagem!")
            return

        sucesso, mensagem = self.cliente.enviar_mensagem_usuario(destinatario, conteudo)

        if sucesso:
            messagebox.showinfo("Sucesso", "Mensagem enviada com sucesso!")
            self.text_mensagem_usuario.delete('1.0', tk.END)
            self.entry_destinatario.delete(0, tk.END)
        else:
            messagebox.showerror("Erro", mensagem)

    def assinar_topico(self):
        """Assina um tópico"""
        topico = self.entry_topico_assinar.get().strip()

        if not topico:
            messagebox.showwarning("Aviso", "Digite o nome do tópico!")
            return

        if topico in self.cliente.topicos_assinados:
            messagebox.showwarning("Aviso", "Você já está inscrito neste tópico!")
            return

        sucesso, mensagem = self.cliente.assinar_topico(topico)

        if sucesso:
            messagebox.showinfo("Sucesso", mensagem)
            self.entry_topico_assinar.delete(0, tk.END)
            self.atualizar_lista_topicos()

            # Reiniciar o consumo para incluir o novo tópico
            self.cliente.consuming = False
            time.sleep(0.5)
            self.cliente.iniciar_consumo(self.processar_mensagem_recebida)
        else:
            messagebox.showerror("Erro", mensagem)

    def enviar_mensagem_topico(self):
        """Envia mensagem para um tópico"""
        topico = self.entry_topico_enviar.get().strip()
        conteudo = self.text_mensagem_topico.get('1.0', tk.END).strip()

        if not topico:
            messagebox.showwarning("Aviso", "Digite o nome do tópico!")
            return

        if not conteudo:
            messagebox.showwarning("Aviso", "Digite uma mensagem!")
            return

        sucesso, mensagem = self.cliente.enviar_mensagem_topico(topico, conteudo)

        if sucesso:
            messagebox.showinfo("Sucesso", "Mensagem enviada para o tópico!")
            self.text_mensagem_topico.delete('1.0', tk.END)
            self.entry_topico_enviar.delete(0, tk.END)
        else:
            messagebox.showerror("Erro", mensagem)

    def processar_mensagem_recebida(self, mensagem):
        """Processa mensagem recebida do RabbitMQ"""

        def atualizar_gui():
            self.mensagens_recebidas.append(mensagem)
            self.exibir_mensagem(mensagem)

        # Executar no thread principal
        self.root.after(0, atualizar_gui)

    def exibir_mensagem(self, mensagem):
        """Exibe uma mensagem na área de texto"""
        self.text_mensagens.config(state=tk.NORMAL)

        # Formatar mensagem
        try:
            timestamp = datetime.fromisoformat(mensagem.get('timestamp', '')).strftime('%H:%M:%S')
        except:
            timestamp = datetime.now().strftime('%H:%M:%S')

        tipo = mensagem.get('tipo', 'desconhecido')

        if tipo == 'mensagem_topico':
            # Mensagem de tópico
            topico = mensagem.get('topico', 'Desconhecido')
            remetente = mensagem.get('remetente', 'Desconhecido')
            conteudo = mensagem.get('conteudo', '')

            self.text_mensagens.insert(tk.END, f"[{timestamp}] 📢 TÓPICO '{topico}' - {remetente}:\n")
            self.text_mensagens.insert(tk.END, f"{conteudo}\n")
            self.text_mensagens.insert(tk.END, "=" * 60 + "\n\n")

        elif tipo == 'mensagem_direta':
            # Mensagem direta
            remetente = mensagem.get('remetente', 'Desconhecido')
            conteudo = mensagem.get('conteudo', '')

            self.text_mensagens.insert(tk.END, f"[{timestamp}] 💬 {remetente}:\n")
            self.text_mensagens.insert(tk.END, f"{conteudo}\n")
            self.text_mensagens.insert(tk.END, "-" * 50 + "\n\n")

        self.text_mensagens.config(state=tk.DISABLED)
        self.text_mensagens.see(tk.END)

    def atualizar_lista_topicos(self):
        """Atualiza a lista de tópicos assinados"""
        self.listbox_topicos.delete(0, tk.END)
        for topico in self.cliente.topicos_assinados:
            self.listbox_topicos.insert(tk.END, topico)

    def limpar_mensagens(self):
        """Limpa a área de mensagens"""
        self.text_mensagens.config(state=tk.NORMAL)
        self.text_mensagens.delete('1.0', tk.END)
        self.text_mensagens.config(state=tk.DISABLED)
        self.mensagens_recebidas.clear()

    def fechar_aplicacao(self):
        """Fecha a aplicação e desconecta do RabbitMQ"""
        self.cliente.desconectar()
        self.root.destroy()

    def executar(self):
        """Executa a aplicação"""
        if self.cliente.conectado:
            # Atualizar lista de tópicos inicial
            self.atualizar_lista_topicos()
            self.root.mainloop()


if __name__ == "__main__":
    app = UsuarioGUI()
    app.executar()