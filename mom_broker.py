#!/usr/bin/env python3
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import pika
import json


class MOMGerenciador:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Gerenciador MOM - RabbitMQ")
        self.root.geometry("700x600")
        self.root.protocol("WM_DELETE_WINDOW", self.fechar)

        self.connection = None
        self.channel = None
        self.usuarios = set()  # nomes de usuários conectados
        self.conectar_rabbitmq()

        if self.connection:
            self.criar_interface()
        else:
            messagebox.showerror("Erro", "Não foi possível conectar ao RabbitMQ.")
            self.root.destroy()

    def conectar_rabbitmq(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters('localhost')
            )
            self.channel = self.connection.channel()
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao conectar ao RabbitMQ: {e}")
            self.connection = None

    def criar_interface(self):
        frame = ttk.Frame(self.root, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)

        titulo = ttk.Label(frame, text="Gerenciador MOM", font=("Arial", 16, "bold"))
        titulo.pack(pady=10)

        # Notebooks
        notebook = ttk.Notebook(frame)
        notebook.pack(fill=tk.BOTH, expand=True, pady=10)

        # Aba Filas
        aba_filas = ttk.Frame(notebook)
        notebook.add(aba_filas, text="Filas")
        self.criar_aba_filas(aba_filas)

        # Aba Tópicos
        aba_topicos = ttk.Frame(notebook)
        notebook.add(aba_topicos, text="Tópicos")
        self.criar_aba_topicos(aba_topicos)

        # Aba Usuários
        aba_usuarios = ttk.Frame(notebook)
        notebook.add(aba_usuarios, text="Usuários")
        self.criar_aba_usuarios(aba_usuarios)

        self.status_label = ttk.Label(frame, text="Conectado ao RabbitMQ", foreground="green")
        self.status_label.pack(pady=5)

    def criar_aba_filas(self, parent):
        ttk.Label(parent, text="Gerenciamento de Filas", font=("Arial", 12, "bold")).pack(anchor=tk.W, pady=10)

        # Campo nome da fila
        self.entry_fila = ttk.Entry(parent, width=40)
        self.entry_fila.pack(pady=5)

        ttk.Button(parent, text="Adicionar Fila", command=self.adicionar_fila).pack()
        ttk.Button(parent, text="Remover Fila", command=self.remover_fila).pack()

        ttk.Button(parent, text="Listar Filas", command=self.listar_filas).pack(pady=5)

        self.lista_filas = tk.Listbox(parent, height=10)
        self.lista_filas.pack(fill=tk.BOTH, expand=True, pady=10)

    def criar_aba_topicos(self, parent):
        ttk.Label(parent, text="Gerenciamento de Tópicos", font=("Arial", 12, "bold")).pack(anchor=tk.W, pady=10)

        self.entry_topico = ttk.Entry(parent, width=40)
        self.entry_topico.pack(pady=5)

        ttk.Button(parent, text="Adicionar Tópico", command=self.adicionar_topico).pack()
        ttk.Button(parent, text="Remover Tópico", command=self.remover_topico).pack()

        ttk.Button(parent, text="Listar Tópicos", command=self.listar_topicos).pack(pady=5)

        self.lista_topicos = tk.Listbox(parent, height=10)
        self.lista_topicos.pack(fill=tk.BOTH, expand=True, pady=10)

    def criar_aba_usuarios(self, parent):
        ttk.Label(parent, text="Gerenciamento de Usuários", font=("Arial", 12, "bold")).pack(anchor=tk.W, pady=10)

        ttk.Button(parent, text="Adicionar Usuário", command=self.adicionar_usuario).pack(pady=5)
        ttk.Button(parent, text="Listar Usuários", command=self.listar_usuarios).pack()

        self.lista_usuarios = tk.Listbox(parent, height=10)
        self.lista_usuarios.pack(fill=tk.BOTH, expand=True, pady=10)

    def adicionar_fila(self):
        nome = self.entry_fila.get().strip()
        if not nome:
            messagebox.showwarning("Aviso", "Digite um nome para a fila.")
            return
        try:
            self.channel.queue_declare(queue=nome, durable=True)
            messagebox.showinfo("Sucesso", f"Fila '{nome}' criada.")
            self.listar_filas()
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao criar fila: {e}")

    def remover_fila(self):
        nome = self.entry_fila.get().strip()
        if not nome:
            messagebox.showwarning("Aviso", "Digite o nome da fila.")
            return
        try:
            self.channel.queue_delete(queue=nome)
            messagebox.showinfo("Sucesso", f"Fila '{nome}' removida.")
            self.listar_filas()
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao remover fila: {e}")

    def listar_filas(self):
        """Lista as filas existentes no RabbitMQ"""
        self.lista_filas.delete(0, tk.END)
        try:
            import requests
            from requests.auth import HTTPBasicAuth

            url = "http://localhost:15672/api/queues"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                filas = response.json()
                for fila in filas:
                    if not fila['name'].startswith("amq."):  # ignora filas internas
                        mensagens = fila.get('messages', 0)
                        self.lista_filas.insert(tk.END, f"{fila['name']} - {mensagens} mensagens")
            else:
                messagebox.showerror("Erro", "Não foi possível listar filas. Verifique RabbitMQ Management.")
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao listar filas: {e}")

    def adicionar_topico(self):
        nome = self.entry_topico.get().strip()
        if not nome:
            messagebox.showwarning("Aviso", "Digite um nome para o tópico.")
            return
        try:
            self.channel.exchange_declare(exchange=nome, exchange_type="fanout", durable=True)
            messagebox.showinfo("Sucesso", f"Tópico '{nome}' criado.")
            self.listar_topicos()
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao criar tópico: {e}")

    def remover_topico(self):
        nome = self.entry_topico.get().strip()
        if not nome:
            messagebox.showwarning("Aviso", "Digite o nome do tópico.")
            return
        try:
            self.channel.exchange_delete(exchange=nome)
            messagebox.showinfo("Sucesso", f"Tópico '{nome}' removido.")
            self.listar_topicos()
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao remover tópico: {e}")

    def listar_topicos(self):
        """Lista os tópicos existentes (exchanges do tipo fanout)"""
        self.lista_topicos.delete(0, tk.END)
        try:
            # Consultar tópicos existentes via RabbitMQ Management Plugin (necessário estar ativo)
            import requests
            from requests.auth import HTTPBasicAuth

            url = "http://localhost:15672/api/exchanges"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                exchanges = response.json()
                for ex in exchanges:
                    # Filtra os tópicos criados (fanout, ignorando os internos do RabbitMQ)
                    if ex['type'] == 'fanout' and not ex['name'].startswith("amq."):
                        self.lista_topicos.insert(tk.END, ex['name'])
            else:
                messagebox.showerror("Erro", "Não foi possível listar tópicos. Verifique o RabbitMQ Management.")

        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao listar tópicos: {e}")

    def adicionar_usuario(self):
        nome = simpledialog.askstring("Novo Usuário", "Digite o nome do usuário:")
        if not nome:
            return
        if nome in self.usuarios:
            messagebox.showwarning("Aviso", "Usuário já existe!")
            return
        try:
            fila = f"user_{nome}"
            self.channel.queue_declare(queue=fila, durable=True)
            self.usuarios.add(nome)
            messagebox.showinfo("Sucesso", f"Usuário '{nome}' criado com fila '{fila}'.")
            self.listar_usuarios()
            self.listar_filas()
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao criar usuário: {e}")

    def listar_usuarios(self):
        self.lista_usuarios.delete(0, tk.END)
        for usuario in self.usuarios:
            self.lista_usuarios.insert(tk.END, usuario)

    def fechar(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
        self.root.destroy()

    def executar(self):
        self.root.mainloop()


if __name__ == "__main__":
    app = MOMGerenciador()
    app.executar()
