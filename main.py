#!/usr/bin/env python3
"""
Script Otimizado para Reconciliação de Bases de Dados
====================================================

Versão otimizada usando funções nativas do pandas como merge, compare, etc.
Muito mais eficiente para grandes volumes de dados.

Autor: Pedro Godoy with Manus AI help
Data: 18/07/2025
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path
from datetime import datetime
import argparse


class ReconciliadorBasesOtimizado:
    """Classe otimizada para reconciliar duas bases de dados usando pandas nativo"""
    
    def __init__(self, arquivo_base1, arquivo_base2, chave_primaria=None):
        """
        Inicializa o reconciliador otimizado
        
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
        self.divergencias_df = None
        self.resumo_stats = {}
        
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
            
            # Armazena estatísticas
            self.resumo_stats['linhas_base1'] = len(self.df1)
            self.resumo_stats['linhas_base2'] = len(self.df2)
            self.resumo_stats['colunas_base1'] = len(self.df1.columns)
            self.resumo_stats['colunas_base2'] = len(self.df2.columns)
            
        except Exception as e:
            print(f"❌ Erro ao carregar dados: {e}")
            sys.exit(1)
    
    def validar_estrutura(self):
        """Valida se as estruturas das bases são compatíveis"""
        # Verifica se as colunas são iguais
        colunas_base1 = set(self.df1.columns)
        colunas_base2 = set(self.df2.columns)
        
        self.resumo_stats['colunas_faltando_base2'] = list(colunas_base1 - colunas_base2)
        self.resumo_stats['colunas_faltando_base1'] = list(colunas_base2 - colunas_base1)
        
        if colunas_base1 != colunas_base2:
            print("⚠️  ATENÇÃO: Estruturas diferentes detectadas!")
            if self.resumo_stats['colunas_faltando_base2']:
                print(f"   Colunas presentes na Base 1 mas ausentes na Base 2: {self.resumo_stats['colunas_faltando_base2']}")
            if self.resumo_stats['colunas_faltando_base1']:
                print(f"   Colunas presentes na Base 2 mas ausentes na Base 1: {self.resumo_stats['colunas_faltando_base1']}")
            
            # Mantém apenas colunas comuns
            colunas_comuns = list(colunas_base1.intersection(colunas_base2))
            self.df1 = self.df1[colunas_comuns]
            self.df2 = self.df2[colunas_comuns]
            print(f"   Continuando com {len(colunas_comuns)} colunas comuns")
        else:
            print("✓ Estruturas das bases são idênticas")
    
    def preparar_dados_para_comparacao(self):
        """Prepara os dados para comparação otimizada"""
        # Adiciona identificador de origem
        df1_prep = self.df1.copy()
        df2_prep = self.df2.copy()
        
        if self.chave_primaria is None:
            # Se não há chave primária, usa o índice
            print("ℹ️  Usando índice das linhas como chave de comparação")
            df1_prep['__linha_original__'] = df1_prep.index
            df2_prep['__linha_original__'] = df2_prep.index
            chave_merge = '__linha_original__'
        else:
            # Usa chave primária definida
            if isinstance(self.chave_primaria, str):
                chave_merge = self.chave_primaria
                print(f"ℹ️  Usando como chave primária: {self.chave_primaria}")
            else:
                chave_merge = self.chave_primaria
                print(f"ℹ️  Usando como chave primária: {self.chave_primaria}")
        
        return df1_prep, df2_prep, chave_merge
    
    def identificar_divergencias_com_merge(self):
        """Usa pandas merge para identificar divergências de forma otimizada"""
        print("\n🔍 Iniciando comparação otimizada com pandas...")
        
        df1_prep, df2_prep, chave_merge = self.preparar_dados_para_comparacao()
        
        # Realiza merge outer para identificar todas as combinações
        merged = pd.merge(
            df1_prep, df2_prep, 
            on=chave_merge, 
            how='outer', 
            suffixes=('_base1', '_base2'),
            indicator=True
        )
        
        divergencias_lista = []
        
        # 1. Identifica linhas que existem apenas em uma base
        linhas_so_base1 = merged[merged['_merge'] == 'left_only']
        linhas_so_base2 = merged[merged['_merge'] == 'right_only']
        
        for _, linha in linhas_so_base1.iterrows():
            chave_valor = linha[chave_merge] if isinstance(chave_merge, str) else [linha[col] for col in chave_merge]
            divergencias_lista.append({
                'chave': str(chave_valor),
                'tipo': 'LINHA_FALTANDO_BASE2',
                'coluna': 'TODAS',
                'valor_base1': 'LINHA_EXISTE',
                'valor_base2': 'LINHA_INEXISTENTE',
                'descricao': f'Linha com chave {chave_valor} existe na Base 1 mas não na Base 2'
            })
        
        for _, linha in linhas_so_base2.iterrows():
            chave_valor = linha[chave_merge] if isinstance(chave_merge, str) else [linha[col] for col in chave_merge]
            divergencias_lista.append({
                'chave': str(chave_valor),
                'tipo': 'LINHA_FALTANDO_BASE1',
                'coluna': 'TODAS',
                'valor_base1': 'LINHA_INEXISTENTE',
                'valor_base2': 'LINHA_EXISTE',
                'descricao': f'Linha com chave {chave_valor} existe na Base 2 mas não na Base 1'
            })
        
        # 2. Para linhas que existem em ambas as bases, compara valores
        linhas_ambas = merged[merged['_merge'] == 'both']
        
        # Identifica colunas para comparação (exclui colunas auxiliares)
        colunas_comparacao = [col for col in self.df1.columns if col != chave_merge and col != '__linha_original__']
        
        for _, linha in linhas_ambas.iterrows():
            chave_valor = linha[chave_merge] if isinstance(chave_merge, str) else [linha[col] for col in chave_merge]
            
            for coluna in colunas_comparacao:
                col_base1 = f"{coluna}_base1"
                col_base2 = f"{coluna}_base2"
                
                valor1 = linha[col_base1]
                valor2 = linha[col_base2]
                
                # Trata valores NaN
                if pd.isna(valor1) and pd.isna(valor2):
                    continue
                
                # Verifica se são diferentes
                if pd.isna(valor1) or pd.isna(valor2) or valor1 != valor2:
                    divergencias_lista.append({
                        'chave': str(chave_valor),
                        'tipo': 'VALOR_DIFERENTE',
                        'coluna': coluna,
                        'valor_base1': valor1 if not pd.isna(valor1) else 'NULL',
                        'valor_base2': valor2 if not pd.isna(valor2) else 'NULL',
                        'descricao': f'Divergência na chave {chave_valor}, coluna "{coluna}"'
                    })
        
        # Converte para DataFrame
        if divergencias_lista:
            self.divergencias_df = pd.DataFrame(divergencias_lista)
        else:
            self.divergencias_df = pd.DataFrame()
        
        # Atualiza estatísticas
        self.resumo_stats['total_divergencias'] = len(divergencias_lista)
        self.resumo_stats['linhas_so_base1'] = len(linhas_so_base1)
        self.resumo_stats['linhas_so_base2'] = len(linhas_so_base2)
        self.resumo_stats['linhas_comuns'] = len(linhas_ambas)
    
    def usar_pandas_compare_quando_possivel(self):
        """Usa pandas.compare() quando as estruturas são idênticas"""
        if (len(self.df1) == len(self.df2) and 
            list(self.df1.columns) == list(self.df2.columns) and
            self.chave_primaria is None):
            
            print("🚀 Usando pandas.compare() para comparação ultra-rápida...")
            
            try:
                # Usa compare() do pandas (disponível a partir da versão 1.1.0)
                comparison = self.df1.compare(self.df2, align_axis=0, keep_shape=True, keep_equal=False)
                
                if not comparison.empty:
                    print(f"📊 pandas.compare() detectou diferenças em {len(comparison)} posições")
                    
                    # Converte resultado do compare para formato padrão
                    divergencias_lista = []
                    
                    for idx in comparison.index:
                        for col in comparison.columns.get_level_values(0).unique():
                            if (col, 'self') in comparison.columns and (col, 'other') in comparison.columns:
                                val_self = comparison.loc[idx, (col, 'self')]
                                val_other = comparison.loc[idx, (col, 'other')]
                                
                                if not (pd.isna(val_self) and pd.isna(val_other)):
                                    divergencias_lista.append({
                                        'chave': str(idx),
                                        'tipo': 'VALOR_DIFERENTE',
                                        'coluna': col,
                                        'valor_base1': val_self if not pd.isna(val_self) else 'NULL',
                                        'valor_base2': val_other if not pd.isna(val_other) else 'NULL',
                                        'descricao': f'Divergência na linha {idx}, coluna "{col}"'
                                    })
                    
                    self.divergencias_df = pd.DataFrame(divergencias_lista)
                    self.resumo_stats['total_divergencias'] = len(divergencias_lista)
                    return True
                else:
                    print("✅ pandas.compare() confirmou: bases são idênticas!")
                    self.divergencias_df = pd.DataFrame()
                    self.resumo_stats['total_divergencias'] = 0
                    return True
                    
            except Exception as e:
                print(f"⚠️  pandas.compare() não disponível ou falhou: {e}")
                print("   Usando método alternativo...")
                return False
        
        return False
    
    def gerar_relatorio_otimizado(self, arquivo_saida=None):
        """Gera relatório das divergências de forma otimizada"""
        if self.divergencias_df.empty:
            print("\n✅ RECONCILIAÇÃO CONCLUÍDA: Nenhuma divergência encontrada!")
            print("   As duas bases são idênticas.")
            return
        
        total_divergencias = len(self.divergencias_df)
        print(f"\n📊 RELATÓRIO DE DIVERGÊNCIAS")
        print(f"   Total de divergências encontradas: {total_divergencias}")
        
        # Agrupa por tipo usando pandas
        tipos_divergencia = self.divergencias_df['tipo'].value_counts().to_dict()
        
        print("\n📈 Resumo por tipo:")
        for tipo, quantidade in tipos_divergencia.items():
            print(f"   {tipo}: {quantidade} ocorrências")
        
        # Define arquivo de saída
        if arquivo_saida is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            arquivo_saida = f"relatorio_divergencias_otimizado_{timestamp}.xlsx"
        
        # Salva relatório usando pandas nativo
        try:
            with pd.ExcelWriter(arquivo_saida, engine='openpyxl') as writer:
                # Aba principal com todas as divergências
                self.divergencias_df.to_excel(writer, sheet_name='Divergencias', index=False)
                
                # Aba com resumo detalhado
                resumo_data = []
                for chave, valor in self.resumo_stats.items():
                    resumo_data.append([chave.replace('_', ' ').title(), valor])
                resumo_data.append(['Data/Hora da análise', datetime.now().strftime("%d/%m/%Y %H:%M:%S")])
                
                df_resumo = pd.DataFrame(resumo_data, columns=['Métrica', 'Valor'])
                df_resumo.to_excel(writer, sheet_name='Resumo', index=False)
                
                # Aba com estatísticas por tipo
                df_tipos = pd.DataFrame(list(tipos_divergencia.items()), 
                                      columns=['Tipo_Divergencia', 'Quantidade'])
                df_tipos.to_excel(writer, sheet_name='Estatisticas_Tipo', index=False)
                
                # Abas separadas por tipo (usando groupby do pandas)
                for tipo in tipos_divergencia.keys():
                    df_tipo = self.divergencias_df[self.divergencias_df['tipo'] == tipo]
                    sheet_name = tipo[:31]  # Limite do Excel para nome de aba
                    df_tipo.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"\n💾 Relatório salvo em: {arquivo_saida}")
            
        except Exception as e:
            print(f"❌ Erro ao salvar relatório: {e}")
            print("\n📋 Primeiras 10 divergências:")
            print(self.divergencias_df.head(10).to_string(index=False))
    
    def executar_reconciliacao_otimizada(self, arquivo_relatorio=None):
        """Executa todo o processo de reconciliação otimizada"""
        print("🚀 Iniciando reconciliação OTIMIZADA de bases...")
        print(f"   Base 1: {self.arquivo_base1}")
        print(f"   Base 2: {self.arquivo_base2}")
        
        inicio = datetime.now()
        
        self.carregar_dados()
        self.validar_estrutura()
        
        # Tenta usar pandas.compare() primeiro (mais rápido)
        if not self.usar_pandas_compare_quando_possivel():
            # Se não conseguir usar compare(), usa merge otimizado
            self.identificar_divergencias_com_merge()
        
        self.gerar_relatorio_otimizado(arquivo_relatorio)
        
        fim = datetime.now()
        tempo_execucao = (fim - inicio).total_seconds()
        print(f"\n⏱️  Tempo de execução: {tempo_execucao:.2f} segundos")
        
        return self.divergencias_df.empty


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description='Reconcilia duas bases de dados (VERSÃO OTIMIZADA)')
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
    
    # Executa reconciliação otimizada
    reconciliador = ReconciliadorBasesOtimizado(args.base1, args.base2, chave_primaria)
    sucesso = reconciliador.executar_reconciliacao_otimizada(args.relatorio)
    
    sys.exit(0 if sucesso else 1)


if __name__ == "__main__":
    main()

