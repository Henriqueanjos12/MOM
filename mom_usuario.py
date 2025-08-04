import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import pika
import json
import threading
import time
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth


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
        """Conecta ao RabbitMQ apenas se o usuário já existir"""
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters('localhost')
            )
            self.channel = self.connection.channel()

            self.nome_usuario = nome_usuario
            self.fila_pessoal = f"user_{nome_usuario}"

            # Verificar se a fila do usuário existe
            url = "http://localhost:15672/api/queues"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                filas = [fila['name'] for fila in response.json()]
                if self.fila_pessoal not in filas:
                    messagebox.showerror(
                        "Erro",
                        f"Usuário '{nome_usuario}' não existe!\n"
                        "Peça ao administrador para criá-lo no Gerenciador."
                    )
                    return False
            else:
                messagebox.showerror("Erro", "Não foi possível validar o usuário no RabbitMQ.")
                return False

            self.conectado = True
            self.carregar_assinaturas_existentes()
            return True

        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao conectar ao RabbitMQ: {e}")
            return False

    def desconectar(self):
        self.consuming = False
        if self.connection and not self.connection.is_closed:
            try:
                self.connection.close()
            except:
                pass
        self.conectado = False

    def buscar_usuarios_disponiveis(self):
        """Retorna lista de usuários existentes (filas user_)"""
        usuarios = []
        try:
            url = "http://localhost:15672/api/queues"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                filas = response.json()
                for fila in filas:
                    if fila['name'].startswith("user_"):
                        usuario = fila['name'].replace("user_", "")
                        usuarios.append(usuario)
        except Exception as e:
            print(f"Erro ao buscar usuários: {e}")
        return sorted(usuarios)

    def assinar_topico(self, nome_topico):
        try:
            if not self.conectado:
                return False, "Não conectado ao RabbitMQ"

            self.channel.exchange_declare(exchange=nome_topico, exchange_type='fanout', durable=True)
            fila_topico = f"topic_{nome_topico}_{self.nome_usuario}"
            self.channel.queue_declare(queue=fila_topico, durable=True)
            self.channel.queue_bind(exchange=nome_topico, queue=fila_topico)

            self.topicos_assinados.add(nome_topico)
            return True, f"Inscrito no tópico '{nome_topico}'"
        except Exception as e:
            return False, f"Erro ao assinar tópico: {e}"

    def desassinar_topico(self, nome_topico):
        try:
            fila_topico = f"topic_{nome_topico}_{self.nome_usuario}"
            self.channel.queue_delete(queue=fila_topico)
            if nome_topico in self.topicos_assinados:
                self.topicos_assinados.remove(nome_topico)
            return True, f"Assinatura do tópico '{nome_topico}' removida."
        except Exception as e:
            return False, f"Erro ao desassinar tópico: {e}"

    def buscar_topicos_disponiveis(self):
        topicos = []
        try:
            url = "http://localhost:15672/api/exchanges"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                exchanges = response.json()
                for ex in exchanges:
                    if ex['type'] == 'fanout' and not ex['name'].startswith("amq."):
                        topicos.append(ex['name'])
        except Exception as e:
            print(f"Erro ao buscar tópicos: {e}")
        return sorted(topicos)

    def carregar_assinaturas_existentes(self):
        try:
            url = "http://localhost:15672/api/queues"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                filas = response.json()
                for fila in filas:
                    nome_fila = fila['name']
                    if nome_fila.startswith("topic_") and nome_fila.endswith(f"_{self.nome_usuario}"):
                        partes = nome_fila.split("_")
                        if len(partes) >= 3:
                            nome_topico = "_".join(partes[1:-1])
                            self.topicos_assinados.add(nome_topico)
        except Exception as e:
            print(f"Erro ao recuperar assinaturas: {e}")

    def enviar_mensagem_usuario(self, destinatario, conteudo):
        try:
            if not self.conectado:
                return False, "Não conectado ao RabbitMQ"
            fila_destinatario = f"user_{destinatario}"
            url = "http://localhost:15672/api/queues"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                filas = [fila['name'] for fila in response.json()]
                if fila_destinatario not in filas:
                    return False, f"Usuário '{destinatario}' não existe!"

            mensagem = {
                'tipo': 'mensagem_direta',
                'remetente': self.nome_usuario,
                'destinatario': destinatario,
                'conteudo': conteudo,
                'timestamp': datetime.now().isoformat()
            }
            self.channel.basic_publish(
                exchange='', routing_key=fila_destinatario,
                body=json.dumps(mensagem),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            return True, "Mensagem enviada"
        except Exception as e:
            return False, f"Erro ao enviar mensagem: {e}"

    def enviar_mensagem_topico(self, nome_topico, conteudo):
        try:
            if not self.conectado:
                return False, "Não conectado ao RabbitMQ"
            try:
                self.channel.exchange_declare(exchange=nome_topico, exchange_type='fanout', passive=True)
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
                exchange=nome_topico, routing_key='',
                body=json.dumps(mensagem),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            return True, f"Mensagem enviada para o tópico '{nome_topico}'"
        except Exception as e:
            return False, f"Erro ao enviar mensagem para tópico: {e}"

    def iniciar_consumo(self, callback_mensagem):
        if not self.conectado:
            return
        self.callback_mensagem = callback_mensagem
        self.consuming = True
        threading.Thread(target=self._consumir_fila_pessoal, daemon=True).start()
        threading.Thread(target=self._consumir_topicos, daemon=True).start()

    def _consumir_fila_pessoal(self):
        try:
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
                except:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            consumer_channel.basic_qos(prefetch_count=1)
            consumer_channel.basic_consume(
                queue=self.fila_pessoal,
                on_message_callback=callback
            )

            while self.consuming:
                consumer_connection.process_data_events(time_limit=1)

            consumer_connection.close()
        except Exception as e:
            print(f"Erro no consumo da fila pessoal: {e}")

    def _consumir_topicos(self):
        try:
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
                except:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            for topico in self.topicos_assinados:
                fila_topico = f"topic_{topico}_{self.nome_usuario}"
                topic_channel.basic_consume(
                    queue=fila_topico,
                    on_message_callback=callback
                )

            while self.consuming:
                topic_connection.process_data_events(time_limit=1)

            topic_connection.close()
        except Exception as e:
            print(f"Erro no consumo de tópicos: {e}")

    def buscar_filas_gerais(self):
        """Retorna filas que não são de usuários nem de tópicos"""
        filas = []
        try:
            url = "http://localhost:15672/api/queues"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                for fila in response.json():
                    nome = fila['name']
                    if not nome.startswith("user_") and not nome.startswith("topic_") and not nome.startswith("amq."):
                        filas.append(nome)
        except Exception as e:
            print(f"Erro ao buscar filas gerais: {e}")
        return sorted(filas)


class UsuarioGUI:
    def __init__(self, nome_usuario=None):
        self.cliente = RabbitMQCliente()
        self.root = tk.Tk()
        self.root.title("MOM Cliente - RabbitMQ")
        self.root.geometry("900x700")
        self.root.protocol("WM_DELETE_WINDOW", self.fechar_aplicacao)

        self.mensagens_recebidas = []
        self.filas_consumindo = set()
        self.conectar_usuario(nome_usuario)

        if self.cliente.conectado:
            self.criar_interface()
            self.cliente.iniciar_consumo(self.processar_mensagem_recebida)
        else:
            self.root.destroy()
            return

        self.filas_consumindo = set()

    def conectar_usuario(self, nome_usuario=None):
        while True:
            if nome_usuario is None:
                nome = simpledialog.askstring("Login", "Digite seu nome de usuário:", parent=self.root)
            else:
                nome = nome_usuario
            if not nome:
                self.root.destroy()
                return
            nome = nome.strip()
            if nome and self.cliente.conectar(nome):
                self.root.title(f"MOM Cliente - {nome}")
                break
            if nome_usuario is not None:
                self.root.destroy()
                return

    def criar_interface(self):
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        titulo = ttk.Label(main_frame, text=f"Cliente MOM RabbitMQ - {self.cliente.nome_usuario}",
                           font=('Arial', 16, 'bold'))
        titulo.pack(pady=(0, 20))

        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        frame_mensagens = ttk.Frame(notebook)
        notebook.add(frame_mensagens, text="Mensagens Recebidas")
        self.criar_aba_mensagens(frame_mensagens)

        frame_filas = ttk.Frame(notebook)
        notebook.add(frame_filas, text="Filas Gerais")
        self.criar_aba_filas(frame_filas)

        frame_usuario = ttk.Frame(notebook)
        notebook.add(frame_usuario, text="Enviar para Usuário")
        self.criar_aba_enviar_usuario(frame_usuario)

        frame_topicos = ttk.Frame(notebook)
        notebook.add(frame_topicos, text="Tópicos")
        self.criar_aba_topicos(frame_topicos)

        self.status_label = ttk.Label(main_frame, text="Conectado ao RabbitMQ",
                                      foreground="green", font=('Arial', 10))
        self.status_label.pack()

    def criar_aba_mensagens(self, parent):
        frame = ttk.Frame(parent, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="Mensagens Recebidas", font=('Arial', 12, 'bold')).pack(anchor=tk.W, pady=(0, 10))

        frame_mensagens = ttk.Frame(frame)
        frame_mensagens.pack(fill=tk.BOTH, expand=True)

        self.text_mensagens = tk.Text(frame_mensagens, wrap=tk.WORD, state=tk.DISABLED, font=('Arial', 10))
        scrollbar_msg = ttk.Scrollbar(frame_mensagens, orient=tk.VERTICAL, command=self.text_mensagens.yview)
        self.text_mensagens.configure(yscrollcommand=scrollbar_msg.set)

        self.text_mensagens.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar_msg.pack(side=tk.RIGHT, fill=tk.Y)

        ttk.Button(frame, text="Limpar Mensagens", command=self.limpar_mensagens).pack(pady=(10, 0))

    def criar_aba_filas(self, parent):
        frame = ttk.Frame(parent, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="Enviar e Receber de Filas Gerais",
                  font=('Arial', 12, 'bold')).pack(anchor=tk.W, pady=(0, 10))

        ttk.Label(frame, text="Selecione a Fila:").pack(anchor=tk.W)
        self.combo_filas = ttk.Combobox(frame, state="readonly")
        self.combo_filas.pack(fill=tk.X, pady=(0, 10))
        self.atualizar_lista_filas()

        ttk.Button(frame, text="Atualizar Lista de Filas",
                   command=self.atualizar_lista_filas).pack(pady=(0, 10))

        ttk.Label(frame, text="Mensagem:").pack(anchor=tk.W)
        self.text_mensagem_fila = tk.Text(frame, height=6, wrap=tk.WORD, font=('Arial', 10))
        self.text_mensagem_fila.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        ttk.Button(frame, text="Enviar para Fila", command=self.enviar_mensagem_fila).pack(pady=(5, 5))
        ttk.Button(frame, text="Consumir 1 Mensagem", command=self.consumir_uma_mensagem_fila).pack()

    def criar_aba_enviar_usuario(self, parent):
        frame = ttk.Frame(parent, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="Enviar Mensagem para Usuário", font=('Arial', 12, 'bold')).pack(anchor=tk.W, pady=(0, 20))

        ttk.Label(frame, text="Destinatário:").pack(anchor=tk.W)
        self.combo_destinatario = ttk.Combobox(frame, state="readonly", font=('Arial', 11))
        self.combo_destinatario.pack(fill=tk.X, pady=(0, 10))
        self.atualizar_lista_usuarios()

        ttk.Label(frame, text="Mensagem:").pack(anchor=tk.W)
        self.text_mensagem_usuario = tk.Text(frame, height=10, wrap=tk.WORD, font=('Arial', 10))
        self.text_mensagem_usuario.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        ttk.Button(frame, text="Enviar Mensagem", command=self.enviar_mensagem_usuario).pack()

    def criar_aba_topicos(self, parent):
        frame = ttk.Frame(parent, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        # Assinaturas
        frame_assinar = ttk.LabelFrame(frame, text="Gerenciar Assinaturas de Tópicos", padding="10")
        frame_assinar.pack(fill=tk.BOTH, expand=True, pady=(0, 20))

        self.frame_checkboxes = ttk.Frame(frame_assinar)
        self.frame_checkboxes.pack(fill=tk.BOTH, expand=True)

        self.topicos_vars = {}

        # Enviar mensagem
        frame_enviar = ttk.LabelFrame(frame, text="Enviar Mensagem para Tópico", padding="10")
        frame_enviar.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame_enviar, text="Selecione o tópico:").pack(anchor=tk.W)
        self.combo_topicos = ttk.Combobox(frame_enviar, state="readonly")
        self.combo_topicos.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame_enviar, text="Mensagem:").pack(anchor=tk.W)
        self.text_mensagem_topico = tk.Text(frame_enviar, height=6, wrap=tk.WORD, font=('Arial', 10))
        self.text_mensagem_topico.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        ttk.Button(frame_enviar, text="Enviar para Tópico", command=self.enviar_mensagem_topico).pack()

        # Agora sim, atualiza os checkboxes e o combobox
        self.atualizar_checkboxes()

    def atualizar_checkboxes(self):
        for widget in self.frame_checkboxes.winfo_children():
            widget.destroy()
        self.topicos_vars.clear()

        topicos_disponiveis = self.cliente.buscar_topicos_disponiveis()
        for topico in topicos_disponiveis:
            var = tk.BooleanVar(value=topico in self.cliente.topicos_assinados)
            cb = ttk.Checkbutton(self.frame_checkboxes, text=topico, variable=var,
                                 command=lambda t=topico, v=var: self.toggle_topico(t, v))
            cb.pack(anchor=tk.W)
            self.topicos_vars[topico] = var

        # Atualiza combobox com TODOS os tópicos disponíveis
        self.combo_topicos['values'] = self.cliente.buscar_topicos_disponiveis()

    def toggle_topico(self, nome_topico, var):
        if var.get():
            sucesso, msg = self.cliente.assinar_topico(nome_topico)
            if sucesso:
                messagebox.showinfo("Sucesso", msg)
        else:
            sucesso, msg = self.cliente.desassinar_topico(nome_topico)
            if sucesso:
                messagebox.showinfo("Sucesso", msg)
        self.atualizar_checkboxes()

    def buscar_usuarios_disponiveis(self):
        """Retorna a lista de usuários existentes (filas user_)"""
        usuarios = []
        try:
            url = "http://localhost:15672/api/queues"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                filas = response.json()
                for fila in filas:
                    if fila['name'].startswith("user_"):
                        usuario = fila['name'].replace("user_", "")
                        usuarios.append(usuario)
        except Exception as e:
            print(f"Erro ao buscar usuários: {e}")
        return sorted(usuarios)

    def atualizar_lista_usuarios(self):
        usuarios = self.cliente.buscar_usuarios_disponiveis()
        if self.cliente.nome_usuario in usuarios:
            usuarios.remove(self.cliente.nome_usuario)  # não listar o próprio usuário
        self.combo_destinatario['values'] = usuarios

    def enviar_mensagem_usuario(self):
        destinatario = self.combo_destinatario.get().strip()
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
            self.combo_destinatario.set('')
        else:
            messagebox.showerror("Erro", mensagem)

    def enviar_mensagem_topico(self):
        topico = self.combo_topicos.get().strip()
        conteudo = self.text_mensagem_topico.get('1.0', tk.END).strip()
        if not topico:
            messagebox.showwarning("Aviso", "Selecione um tópico para enviar a mensagem!")
            return
        if not conteudo:
            messagebox.showwarning("Aviso", "Digite uma mensagem!")
            return
        sucesso, mensagem = self.cliente.enviar_mensagem_topico(topico, conteudo)
        if sucesso:
            messagebox.showinfo("Sucesso", mensagem)
            self.text_mensagem_topico.delete('1.0', tk.END)
        else:
            messagebox.showerror("Erro", mensagem)

    def processar_mensagem_recebida(self, mensagem):
        def atualizar_gui():
            self.mensagens_recebidas.append(mensagem)
            self.exibir_mensagem(mensagem)
        self.root.after(0, atualizar_gui)

    def exibir_mensagem(self, mensagem):
        self.text_mensagens.config(state=tk.NORMAL)
        try:
            timestamp = datetime.fromisoformat(mensagem.get('timestamp', '')).strftime('%H:%M:%S')
        except:
            timestamp = datetime.now().strftime('%H:%M:%S')
        tipo = mensagem.get('tipo', 'desconhecido')
        if tipo == 'mensagem_topico':
            topico = mensagem.get('topico', 'Desconhecido')
            remetente = mensagem.get('remetente', 'Desconhecido')
            conteudo = mensagem.get('conteudo', '')
            self.text_mensagens.insert(tk.END, f"[{timestamp}] 📢 TÓPICO '{topico}' - {remetente}:\n")
            self.text_mensagens.insert(tk.END, f"{conteudo}\n")
            self.text_mensagens.insert(tk.END, "=" * 60 + "\n\n")
        elif tipo == 'mensagem_direta':
            remetente = mensagem.get('remetente', 'Desconhecido')
            conteudo = mensagem.get('conteudo', '')
            self.text_mensagens.insert(tk.END, f"[{timestamp}] 💬 {remetente}:\n")
            self.text_mensagens.insert(tk.END, f"{conteudo}\n")
            self.text_mensagens.insert(tk.END, "-" * 50 + "\n\n")
        elif tipo == 'mensagem_fila':
            fila = mensagem.get('fila', 'Desconhecida')
            remetente = mensagem.get('remetente', 'Desconhecido')
            conteudo = mensagem.get('conteudo', '')
            self.text_mensagens.insert(tk.END, f"[{timestamp}] 📦 FILA '{fila}' - {remetente}:\n")
            self.text_mensagens.insert(tk.END, f"{conteudo}\n")
            self.text_mensagens.insert(tk.END, "#" * 60 + "\n\n")
        self.text_mensagens.config(state=tk.DISABLED)
        self.text_mensagens.see(tk.END)

    def limpar_mensagens(self):
        self.text_mensagens.config(state=tk.NORMAL)
        self.text_mensagens.delete('1.0', tk.END)
        self.text_mensagens.config(state=tk.DISABLED)
        self.mensagens_recebidas.clear()

    def fechar_aplicacao(self):
        self.cliente.desconectar()
        self.root.destroy()

    def executar(self):
        if self.cliente.conectado:
            self.root.mainloop()

    def atualizar_lista_filas(self):
        filas = self.cliente.buscar_filas_gerais()
        self.combo_filas['values'] = filas
        if filas:
            self.combo_filas.current(0)

    def enviar_mensagem_fila(self):
        fila = self.combo_filas.get().strip()
        conteudo = self.text_mensagem_fila.get('1.0', tk.END).strip()
        if not fila:
            messagebox.showwarning("Aviso", "Selecione uma fila!")
            return
        if not conteudo:
            messagebox.showwarning("Aviso", "Digite uma mensagem!")
            return

        try:
            mensagem = {
                'tipo': 'mensagem_fila',
                'fila': fila,
                'remetente': self.cliente.nome_usuario,
                'conteudo': conteudo,
                'timestamp': datetime.now().isoformat()
            }
            self.cliente.channel.basic_publish(
                exchange='', routing_key=fila,
                body=json.dumps(mensagem),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            messagebox.showinfo("Sucesso", f"Mensagem enviada para a fila '{fila}'")
            self.text_mensagem_fila.delete('1.0', tk.END)

        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao enviar mensagem: {e}")

    def consumir_fila_geral(self, fila):
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            ch = conn.channel()

            def callback(ch, method, properties, body):
                try:
                    mensagem = json.loads(body.decode('utf-8'))
                    self.root.after(0, lambda: self.processar_mensagem_recebida(mensagem))
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=fila, on_message_callback=callback)

            while self.cliente.conectado:
                conn.process_data_events(time_limit=1)

            conn.close()
        except Exception as e:
            print(f"Erro ao consumir fila geral '{fila}': {e}")

    def consumir_uma_mensagem_fila(self):
        fila = self.combo_filas.get().strip()
        if not fila:
            messagebox.showwarning("Aviso", "Selecione uma fila!")
            return
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            ch = conn.channel()

            method_frame, header_frame, body = ch.basic_get(queue=fila, auto_ack=False)
            if method_frame:
                try:
                    mensagem = json.loads(body.decode('utf-8'))
                except:
                    mensagem = {"conteudo": body.decode('utf-8')}
                self.processar_mensagem_recebida(mensagem)
                ch.basic_ack(method_frame.delivery_tag)
            else:
                messagebox.showinfo("Fila Vazia", f"Não há mensagens na fila '{fila}'.")
            conn.close()
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao consumir mensagem: {e}")


if __name__ == "__main__":
    import sys
    nome_usuario = sys.argv[1] if len(sys.argv) > 1 else None
    app = UsuarioGUI(nome_usuario)
    app.executar()
