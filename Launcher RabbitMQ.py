#!/usr/bin/env python3
"""
Script Launcher para o Sistema MOM com RabbitMQ
Permite iniciar o gerenciador ou cliente facilmente
"""

import sys
import subprocess
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import os
import pika
import requests
from requests.auth import HTTPBasicAuth


class MOMLauncher:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("MOM RabbitMQ System Launcher")
        self.root.geometry("500x400")
        self.root.resizable(False, False)

        self.gerenciador_existe = os.path.exists("mom_broker.py")
        self.cliente_existe = os.path.exists("mom_usuario.py")
        self.rabbitmq_ok = self.verificar_rabbitmq()

        self.criar_interface()

    def verificar_rabbitmq(self):
        """Verifica se o RabbitMQ está rodando"""
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters('localhost')
            )
            connection.close()
            return True
        except:
            return False

    def criar_interface(self):
        """Cria a interface do launcher"""
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        titulo = ttk.Label(main_frame, text="Sistema MOM",
                           font=('Arial', 18, 'bold'))
        titulo.pack(pady=(0, 5))

        subtitulo = ttk.Label(main_frame, text="Message-Oriented Middleware com RabbitMQ",
                              font=('Arial', 10))
        subtitulo.pack(pady=(0, 20))

        status_frame = ttk.LabelFrame(main_frame, text="Status do Sistema", padding="10")
        status_frame.pack(fill=tk.X, pady=(0, 20))

        rabbitmq_status = "✓ Conectado" if self.rabbitmq_ok else "✗ Desconectado"
        rabbitmq_color = "green" if self.rabbitmq_ok else "red"

        status_rabbitmq = ttk.Label(status_frame, text=f"RabbitMQ: {rabbitmq_status}",
                                    foreground=rabbitmq_color, font=('Arial', 10, 'bold'))
        status_rabbitmq.pack(anchor=tk.W)

        if not self.rabbitmq_ok:
            ttk.Label(status_frame,
                      text="⚠️ RabbitMQ não está rodando. Instale e inicie o RabbitMQ server.",
                      foreground="red", font=('Arial', 9)).pack(anchor=tk.W, pady=(5, 0))
            ttk.Button(status_frame, text="Verificar RabbitMQ Novamente",
                       command=self.verificar_rabbitmq_novamente).pack(pady=(10, 0))

        gerenciador_status = "✓ Disponível" if self.gerenciador_existe else "✗ Não encontrado"
        cliente_status = "✓ Disponível" if self.cliente_existe else "✗ Não encontrado"

        ttk.Label(status_frame, text=f"Gerenciador: {gerenciador_status}").pack(anchor=tk.W)
        ttk.Label(status_frame, text=f"Cliente: {cliente_status}").pack(anchor=tk.W)

        instrucoes_frame = ttk.LabelFrame(main_frame, text="Como usar", padding="10")
        instrucoes_frame.pack(fill=tk.X, pady=(0, 20))

        instrucoes_texto = """1. Certifique-se de que o RabbitMQ está rodando
2. Inicie o Gerenciador para configurar filas e tópicos
3. Inicie quantos Clientes precisar
4. Use as interfaces para enviar mensagens e gerenciar tópicos"""

        ttk.Label(instrucoes_frame, text=instrucoes_texto,
                  font=('Arial', 9), justify=tk.LEFT).pack(anchor=tk.W)

        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(pady=10)

        self.btn_gerenciador = ttk.Button(btn_frame, text="Iniciar Gerenciador\n(Configuração)",
                                          command=self.iniciar_gerenciador,
                                          width=18)
        self.btn_gerenciador.pack(side=tk.LEFT, padx=(0, 10))

        if not self.gerenciador_existe or not self.rabbitmq_ok:
            self.btn_gerenciador.config(state='disabled')

        self.btn_cliente = ttk.Button(btn_frame, text="Iniciar Cliente\n(Usuário)",
                                      command=self.iniciar_cliente,
                                      width=18)
        self.btn_cliente.pack(side=tk.LEFT)

        if not self.cliente_existe or not self.rabbitmq_ok:
            self.btn_cliente.config(state='disabled')

        info_frame = ttk.LabelFrame(main_frame, text="Informações", padding="10")
        info_frame.pack(fill=tk.X, pady=(20, 0))

        ttk.Label(info_frame, text="Dependências:",
                  font=('Arial', 10, 'bold')).pack(anchor=tk.W)
        ttk.Label(info_frame, text="• Python 3.x", font=('Arial', 9)).pack(anchor=tk.W)
        ttk.Label(info_frame, text="• pika (pip install pika)", font=('Arial', 9)).pack(anchor=tk.W)
        ttk.Label(info_frame, text="• requests (pip install requests)", font=('Arial', 9)).pack(anchor=tk.W)
        ttk.Label(info_frame, text="• RabbitMQ Server", font=('Arial', 9)).pack(anchor=tk.W)

    def verificar_rabbitmq_novamente(self):
        self.rabbitmq_ok = self.verificar_rabbitmq()
        if self.rabbitmq_ok:
            messagebox.showinfo("Sucesso", "RabbitMQ está rodando!")
            for widget in self.root.winfo_children():
                widget.destroy()
            self.criar_interface()
        else:
            messagebox.showerror("Erro",
                                 "RabbitMQ ainda não está acessível.\n\nVerifique se está instalado e rodando.")

    def iniciar_gerenciador(self):
        try:
            subprocess.Popen([sys.executable, "mom_broker.py"])
            messagebox.showinfo("Sucesso", "Gerenciador iniciado com sucesso!")
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao iniciar gerenciador: {e}")

    def iniciar_cliente(self):
        """Inicia um cliente apenas se o usuário já existir"""
        try:
            nome_usuario = simpledialog.askstring("Login", "Digite seu nome de usuário:", parent=self.root)
            if not nome_usuario:
                return

            fila_pessoal = f"user_{nome_usuario}"

            url = "http://localhost:15672/api/queues"
            response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
            if response.status_code == 200:
                filas = [fila['name'] for fila in response.json()]
                if fila_pessoal not in filas:
                    messagebox.showerror("Erro",
                                         f"O usuário '{nome_usuario}' não existe!\n"
                                         "Peça ao administrador para criá-lo no Gerenciador.")
                    return
            else:
                messagebox.showerror("Erro", "Não foi possível validar o usuário no RabbitMQ.")
                return

            subprocess.Popen([sys.executable, "mom_usuario.py", nome_usuario])
            messagebox.showinfo("Sucesso", f"Cliente '{nome_usuario}' iniciado com sucesso!")

        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao iniciar cliente: {e}")

    def executar(self):
        self.root.mainloop()


if __name__ == "__main__":
    app = MOMLauncher()
    app.executar()
