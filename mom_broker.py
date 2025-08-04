#!/usr/bin/env python3
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import pika
import requests
from requests.auth import HTTPBasicAuth


class MOMGerenciador:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Gerenciador MOM - RabbitMQ")
        self.root.geometry("1000x600")
        self.root.protocol("WM_DELETE_WINDOW", self.fechar)

        self.connection = None
        self.channel = None
        self.usuarios = set()
        self.check_vars = {}

        self.conectar_rabbitmq()
        if self.connection:
            self.carregar_usuarios_existentes()
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

        notebook = ttk.Notebook(frame)
        notebook.pack(fill=tk.BOTH, expand=True, pady=10)

        aba_filas = ttk.Frame(notebook)
        notebook.add(aba_filas, text="Filas")
        self.criar_aba_filas(aba_filas)

        aba_topicos = ttk.Frame(notebook)
        notebook.add(aba_topicos, text="Tópicos")
        self.criar_aba_topicos(aba_topicos)

        aba_usuarios = ttk.Frame(notebook)
        notebook.add(aba_usuarios, text="Usuários")
        self.criar_aba_usuarios(aba_usuarios)

        self.status_label = ttk.Label(frame, text="Conectado ao RabbitMQ", foreground="green")
        self.status_label.pack(pady=5)

    # -------- FILAS --------
    def criar_aba_filas(self, parent):
        ttk.Label(parent, text="Gerenciamento de Filas", font=("Arial", 12, "bold")).pack(anchor=tk.W, pady=10)

        self.entry_fila = ttk.Entry(parent, width=40)
        self.entry_fila.pack(pady=5)

        ttk.Button(parent, text="Adicionar Fila", command=self.adicionar_fila).pack()
        ttk.Button(parent, text="Remover Fila", command=self.remover_fila).pack()
        ttk.Button(parent, text="Listar Filas", command=self.listar_filas).pack(pady=5)

        self.lista_filas = tk.Listbox(parent, height=10)
        self.lista_filas.pack(fill=tk.BOTH, expand=True, pady=10)
        self.lista_filas.bind("<<ListboxSelect>>", self.selecionar_fila)

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

        if nome.startswith("user_"):
            messagebox.showwarning("Aviso",
                                   "Não é permitido remover a fila de um usuário diretamente!\n"
                                   "Use a aba Usuários para remover o usuário correspondente.")
            return

        try:
            self.channel.queue_delete(queue=nome)
            messagebox.showinfo("Sucesso", f"Fila '{nome}' removida.")
            self.listar_filas()
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao remover fila: {e}")

    def listar_filas(self):
        self.lista_filas.delete(0, tk.END)
        try:
            url = "http://localhost:15672/api/queues"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                filas = sorted([fila for fila in response.json() if not fila['name'].startswith("amq.")],
                               key=lambda x: x['name'])
                for fila in filas:
                    mensagens = fila.get('messages', 0)
                    self.lista_filas.insert(tk.END, f"{fila['name']} - {mensagens} mensagens")
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao listar filas: {e}")

    def selecionar_fila(self, event):
        selecao = self.lista_filas.curselection()
        if selecao:
            texto = self.lista_filas.get(selecao[0])
            nome_fila = texto.split(" - ")[0]
            self.entry_fila.delete(0, tk.END)
            self.entry_fila.insert(0, nome_fila)

    # -------- TÓPICOS --------
    def criar_aba_topicos(self, parent):
        ttk.Label(parent, text="Gerenciamento de Tópicos", font=("Arial", 12, "bold")).pack(anchor=tk.W, pady=10)

        self.entry_topico = ttk.Entry(parent, width=40)
        self.entry_topico.pack(pady=5)

        ttk.Button(parent, text="Adicionar Tópico", command=self.adicionar_topico).pack()
        ttk.Button(parent, text="Remover Tópico", command=self.remover_topico).pack()
        ttk.Button(parent, text="Listar Tópicos", command=self.listar_topicos).pack(pady=5)

        self.lista_topicos = tk.Listbox(parent, height=10)
        self.lista_topicos.pack(fill=tk.BOTH, expand=True, pady=10)
        self.lista_topicos.bind("<<ListboxSelect>>", self.selecionar_topico)

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
        self.lista_topicos.delete(0, tk.END)
        try:
            url = "http://localhost:15672/api/exchanges"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                exchanges = sorted(
                    [ex for ex in response.json() if ex['type'] == 'fanout' and not ex['name'].startswith("amq.")],
                    key=lambda x: x['name'])
                for ex in exchanges:
                    self.lista_topicos.insert(tk.END, ex['name'])
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao listar tópicos: {e}")

    def selecionar_topico(self, event):
        selecao = self.lista_topicos.curselection()
        if selecao:
            nome_topico = self.lista_topicos.get(selecao[0])
            self.entry_topico.delete(0, tk.END)
            self.entry_topico.insert(0, nome_topico)

    # -------- USUÁRIOS --------
    def criar_aba_usuarios(self, parent):
        ttk.Label(parent, text="Gerenciamento de Usuários e Assinaturas", font=("Arial", 12, "bold")).pack(anchor=tk.W,
                                                                                                           pady=10)

        btn_frame = ttk.Frame(parent)
        btn_frame.pack(pady=5)

        ttk.Button(btn_frame, text="Adicionar Usuário", command=self.adicionar_usuario).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Remover Usuário", command=self.remover_usuario).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Atualizar Assinaturas", command=self.atualizar_tabela).pack(side=tk.LEFT, padx=5)

        self.tabela_frame = ttk.Frame(parent)
        self.tabela_frame.pack(fill=tk.BOTH, expand=True, pady=10)

        self.atualizar_tabela()

    def atualizar_tabela(self):
        for widget in self.tabela_frame.winfo_children():
            widget.destroy()

        usuarios = sorted(self.usuarios)
        topicos = self.buscar_topicos_disponiveis()

        ttk.Label(self.tabela_frame, text="Usuário", width=20, anchor="center").grid(row=0, column=0, padx=5, pady=5)
        for j, topico in enumerate(topicos, start=1):
            ttk.Label(self.tabela_frame, text=topico, width=15, anchor="center").grid(row=0, column=j, padx=5, pady=5)

        self.check_vars = {}
        for i, usuario in enumerate(usuarios, start=1):
            ttk.Label(self.tabela_frame, text=usuario, width=20).grid(row=i, column=0, padx=5, pady=5)
            self.check_vars[usuario] = {}
            for j, topico in enumerate(topicos, start=1):
                var = tk.BooleanVar(value=self.verificar_assinatura(usuario, topico))
                cb = ttk.Checkbutton(self.tabela_frame, variable=var,
                                     command=lambda u=usuario, t=topico, v=var: self.toggle_assinatura(u, t, v))
                cb.grid(row=i, column=j, padx=5, pady=5)
                self.check_vars[usuario][topico] = var

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

    def verificar_assinatura(self, usuario, topico):
        fila = f"topic_{topico}_{usuario}"
        try:
            url = "http://localhost:15672/api/queues"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                filas = [f['name'] for f in response.json()]
                return fila in filas
        except:
            return False
        return False

    def toggle_assinatura(self, usuario, topico, var):
        if var.get():
            try:
                self.channel.exchange_declare(exchange=topico, exchange_type='fanout', durable=True)
                fila_topico = f"topic_{topico}_{usuario}"
                self.channel.queue_declare(queue=fila_topico, durable=True)
                self.channel.queue_bind(exchange=topico, queue=fila_topico)
                messagebox.showinfo("Sucesso", f"Usuário '{usuario}' inscrito no tópico '{topico}'")
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao assinar tópico: {e}")
        else:
            try:
                fila_topico = f"topic_{topico}_{usuario}"
                self.channel.queue_delete(queue=fila_topico)
                messagebox.showinfo("Sucesso", f"Usuário '{usuario}' removido do tópico '{topico}'")
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao remover assinatura: {e}")

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
            self.atualizar_tabela()
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao criar usuário: {e}")

    def remover_usuario(self):
        if not self.usuarios:
            messagebox.showwarning("Aviso", "Nenhum usuário para remover!")
            return

        nome = simpledialog.askstring("Remover Usuário", "Digite o nome do usuário a remover:")
        if not nome or nome not in self.usuarios:
            messagebox.showwarning("Aviso", "Usuário não encontrado!")
            return

        if messagebox.askyesno("Confirmação", f"Tem certeza que deseja remover o usuário '{nome}'?"):
            try:
                fila_pessoal = f"user_{nome}"
                self.channel.queue_delete(queue=fila_pessoal)

                url = "http://localhost:15672/api/queues"
                response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
                if response.status_code == 200:
                    filas = response.json()
                    for fila in filas:
                        if fila['name'].endswith(f"_{nome}"):
                            self.channel.queue_delete(queue=fila['name'])

                self.usuarios.discard(nome)
                messagebox.showinfo("Sucesso", f"Usuário '{nome}' removido com sucesso!")
                self.atualizar_tabela()
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao remover usuário: {e}")

    def carregar_usuarios_existentes(self):
        try:
            url = "http://localhost:15672/api/queues"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                filas = response.json()
                for fila in filas:
                    if fila['name'].startswith("user_"):
                        usuario = fila['name'].replace("user_", "")
                        self.usuarios.add(usuario)
        except Exception as e:
            print(f"Não foi possível carregar usuários existentes: {e}")

    def fechar(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
        self.root.destroy()

    def executar(self):
        self.root.mainloop()


if __name__ == "__main__":
    app = MOMGerenciador()
    app.executar()
