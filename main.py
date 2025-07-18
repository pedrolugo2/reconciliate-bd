#!/usr/bin/env python3
"""
Script para Reconciliação de Bases de Dados
============================================

Este script compara duas bases de dados e identifica divergências
coluna por coluna e linha por linha.

Autor: Manus AI
Data: 18/07/2025
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path
from datetime import datetime
import argparse


class ReconciliadorBases:
    """Classe para reconciliar duas bases de dados"""
    
    def __init__(self, arquivo_base1, arquivo_base2, chave_primaria=None):
        """
        Inicializa o reconciliador
        
        Args:
            arquivo_base1 (str): Caminho para o primeiro arquivo
            arquivo_base2 (str): Caminho para o segundo arquivo
            chave_primaria (str ou list): Coluna(s) que servem como chave primária
        """
        self.arquivo_base1 = arquivo_base1
        self.arquivo_base2 = arquivo_base2
        self.chave_primaria = chave_primaria
        self.df1 = None
        self.df2 = None
        self.divergencias = []
        
    def carregar_dados(self):
        """Carrega os dados dos arquivos"""
        try:
            # Detecta o tipo de arquivo e carrega adequadamente
            if self.arquivo_base1.endswith('.csv'):
                self.df1 = pd.read_csv(self.arquivo_base1)
            elif self.arquivo_base1.endswith(('.xlsx', '.xls')):
                self.df1 = pd.read_excel(self.arquivo_base1)
            else:
                raise ValueError(f"Formato não suportado: {self.arquivo_base1}")
                
            if self.arquivo_base2.endswith('.csv'):
                self.df2 = pd.read_csv(self.arquivo_base2)
            elif self.arquivo_base2.endswith(('.xlsx', '.xls')):
                self.df2 = pd.read_excel(self.arquivo_base2)
            else:
                raise ValueError(f"Formato não suportado: {self.arquivo_base2}")
                
            print(f"✓ Base 1 carregada: {len(self.df1)} linhas, {len(self.df1.columns)} colunas")
            print(f"✓ Base 2 carregada: {len(self.df2)} linhas, {len(self.df2.columns)} colunas")
            
        except Exception as e:
            print(f"❌ Erro ao carregar dados: {e}")
            sys.exit(1)
    
    def validar_estrutura(self):
        """Valida se as estruturas das bases são compatíveis"""
        # Verifica se as colunas são iguais
        colunas_base1 = set(self.df1.columns)
        colunas_base2 = set(self.df2.columns)
        
        if colunas_base1 != colunas_base2:
            colunas_faltando_base2 = colunas_base1 - colunas_base2
            colunas_faltando_base1 = colunas_base2 - colunas_base1
            
            print("⚠️  ATENÇÃO: Estruturas diferentes detectadas!")
            if colunas_faltando_base2:
                print(f"   Colunas presentes na Base 1 mas ausentes na Base 2: {colunas_faltando_base2}")
            if colunas_faltando_base1:
                print(f"   Colunas presentes na Base 2 mas ausentes na Base 1: {colunas_faltando_base1}")
            
            # Mantém apenas colunas comuns
            colunas_comuns = list(colunas_base1.intersection(colunas_base2))
            self.df1 = self.df1[colunas_comuns]
            self.df2 = self.df2[colunas_comuns]
            print(f"   Continuando com {len(colunas_comuns)} colunas comuns")
        else:
            print("✓ Estruturas das bases são idênticas")
    
    def preparar_dados(self):
        """Prepara os dados para comparação"""
        # Se não há chave primária definida, usa o índice
        if self.chave_primaria is None:
            print("ℹ️  Usando índice das linhas como chave de comparação")
            self.df1 = self.df1.reset_index(drop=True)
            self.df2 = self.df2.reset_index(drop=True)
        else:
            # Ordena por chave primária
            if isinstance(self.chave_primaria, str):
                chaves = [self.chave_primaria]
            else:
                chaves = self.chave_primaria
                
            print(f"ℹ️  Usando como chave primária: {chaves}")
            self.df1 = self.df1.sort_values(chaves).reset_index(drop=True)
            self.df2 = self.df2.sort_values(chaves).reset_index(drop=True)
    
    def comparar_linha_por_linha(self):
        """Compara as bases linha por linha"""
        print("\n🔍 Iniciando comparação linha por linha...")
        
        max_linhas = max(len(self.df1), len(self.df2))
        
        for linha in range(max_linhas):
            # Verifica se a linha existe em ambas as bases
            existe_base1 = linha < len(self.df1)
            existe_base2 = linha < len(self.df2)
            
            if not existe_base1:
                self.divergencias.append({
                    'linha': linha + 1,
                    'tipo': 'LINHA_FALTANDO_BASE1',
                    'coluna': 'TODAS',
                    'valor_base1': 'LINHA_INEXISTENTE',
                    'valor_base2': 'LINHA_EXISTE',
                    'descricao': f'Linha {linha + 1} existe na Base 2 mas não na Base 1'
                })
                continue
                
            if not existe_base2:
                self.divergencias.append({
                    'linha': linha + 1,
                    'tipo': 'LINHA_FALTANDO_BASE2',
                    'coluna': 'TODAS',
                    'valor_base1': 'LINHA_EXISTE',
                    'valor_base2': 'LINHA_INEXISTENTE',
                    'descricao': f'Linha {linha + 1} existe na Base 1 mas não na Base 2'
                })
                continue
            
            # Compara coluna por coluna
            for coluna in self.df1.columns:
                valor1 = self.df1.iloc[linha][coluna]
                valor2 = self.df2.iloc[linha][coluna]
                
                # Trata valores NaN
                if pd.isna(valor1) and pd.isna(valor2):
                    continue
                
                # Verifica se são diferentes
                if pd.isna(valor1) or pd.isna(valor2) or valor1 != valor2:
                    self.divergencias.append({
                        'linha': linha + 1,
                        'tipo': 'VALOR_DIFERENTE',
                        'coluna': coluna,
                        'valor_base1': valor1 if not pd.isna(valor1) else 'NULL',
                        'valor_base2': valor2 if not pd.isna(valor2) else 'NULL',
                        'descricao': f'Divergência na linha {linha + 1}, coluna "{coluna}"'
                    })
    
    def gerar_relatorio(self, arquivo_saida=None):
        """Gera relatório das divergências"""
        if not self.divergencias:
            print("\n✅ RECONCILIAÇÃO CONCLUÍDA: Nenhuma divergência encontrada!")
            print("   As duas bases são idênticas.")
            return
        
        print(f"\n📊 RELATÓRIO DE DIVERGÊNCIAS")
        print(f"   Total de divergências encontradas: {len(self.divergencias)}")
        
        # Agrupa por tipo de divergência
        tipos_divergencia = {}
        for div in self.divergencias:
            tipo = div['tipo']
            if tipo not in tipos_divergencia:
                tipos_divergencia[tipo] = 0
            tipos_divergencia[tipo] += 1
        
        print("\n📈 Resumo por tipo:")
        for tipo, quantidade in tipos_divergencia.items():
            print(f"   {tipo}: {quantidade} ocorrências")
        
        # Cria DataFrame com as divergências
        df_divergencias = pd.DataFrame(self.divergencias)
        
        # Define arquivo de saída
        if arquivo_saida is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            arquivo_saida = f"relatorio_divergencias_{timestamp}.xlsx"
        
        # Salva relatório
        try:
            with pd.ExcelWriter(arquivo_saida, engine='openpyxl') as writer:
                # Aba principal com todas as divergências
                df_divergencias.to_excel(writer, sheet_name='Divergencias', index=False)
                
                # Aba com resumo
                df_resumo = pd.DataFrame([
                    ['Total de linhas - Base 1', len(self.df1)],
                    ['Total de linhas - Base 2', len(self.df2)],
                    ['Total de colunas', len(self.df1.columns)],
                    ['Total de divergências', len(self.divergencias)],
                    ['Data/Hora da análise', datetime.now().strftime("%d/%m/%Y %H:%M:%S")]
                ], columns=['Métrica', 'Valor'])
                df_resumo.to_excel(writer, sheet_name='Resumo', index=False)
                
                # Aba com detalhes por tipo
                for tipo in tipos_divergencia.keys():
                    df_tipo = df_divergencias[df_divergencias['tipo'] == tipo]
                    sheet_name = tipo[:31]  # Limite do Excel para nome de aba
                    df_tipo.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"\n💾 Relatório salvo em: {arquivo_saida}")
            
        except Exception as e:
            print(f"❌ Erro ao salvar relatório: {e}")
            print("\n📋 Primeiras 10 divergências:")
            for i, div in enumerate(self.divergencias[:10]):
                print(f"   {i+1}. Linha {div['linha']}, Coluna '{div['coluna']}': "
                      f"Base1='{div['valor_base1']}' vs Base2='{div['valor_base2']}'")
    
    def executar_reconciliacao(self, arquivo_relatorio=None):
        """Executa todo o processo de reconciliação"""
        print("🚀 Iniciando reconciliação de bases...")
        print(f"   Base 1: {self.arquivo_base1}")
        print(f"   Base 2: {self.arquivo_base2}")
        
        self.carregar_dados()
        self.validar_estrutura()
        self.preparar_dados()
        self.comparar_linha_por_linha()
        self.gerar_relatorio(arquivo_relatorio)
        
        return len(self.divergencias) == 0


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description='Reconcilia duas bases de dados')
    parser.add_argument('base1', help='Caminho para a primeira base')
    parser.add_argument('base2', help='Caminho para a segunda base')
    parser.add_argument('--chave', '-k', help='Coluna(s) chave primária (separadas por vírgula)')
    parser.add_argument('--relatorio', '-r', help='Nome do arquivo de relatório')
    
    args = parser.parse_args()
    
    # Processa chave primária
    chave_primaria = None
    if args.chave:
        chave_primaria = [col.strip() for col in args.chave.split(',')]
        if len(chave_primaria) == 1:
            chave_primaria = chave_primaria[0]
    
    # Executa reconciliação
    reconciliador = ReconciliadorBases(args.base1, args.base2, chave_primaria)
    sucesso = reconciliador.executar_reconciliacao(args.relatorio)
    
    sys.exit(0 if sucesso else 1)


if __name__ == "__main__":
    main()
