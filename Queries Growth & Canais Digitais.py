# Databricks notebook source
# MAGIC %sql
# MAGIC select * from gold.fact_volumeliterstlogMonthly
# MAGIC order by ReferenceDate desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC   from gold.fact_volumeliterstlogMonthly c 
# MAGIC   where c.FuelType NOT IN ("Others") and  c.ReferenceDate >= trunc(add_months(current_date(), -2), 'MM')
# MAGIC
# MAGIC   order by ReferenceDate desc

# COMMAND ----------

# DBTITLE 1,Tratamento base
# MAGIC %sql
# MAGIC select 
# MAGIC ` CNPJ `
# MAGIC  -- Remove pontuação
# MAGIC , regexp_replace(` CNPJ `, '[^0-9]', '') AS CNPJ_LIMPO
# MAGIC , lpad(regexp_replace(` CNPJ `, '[^0-9]', ''), 14, '0') AS CNPJ_FORMATADO
# MAGIC , length(lpad(regexp_replace(` CNPJ `, '[^0-9]', ''), 14, '0')) AS QTD_CARACTERES
# MAGIC
# MAGIC from incentivos.pesquisa_ticket_log_cris

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.fact_revenuetlogmonthly
# MAGIC where SK_Registration in ('-7043861431039944367',
# MAGIC '-5880194871038386625',
# MAGIC '-3589949986037568811',
# MAGIC '6704114796494351902',
# MAGIC '7536790730565048711',
# MAGIC '7919354738276690922')

# COMMAND ----------

# MAGIC %sql
# MAGIC   select 
# MAGIC   *
# MAGIC --, rev.RevenueTotal as receita 
# MAGIC
# MAGIC   from gold.dim_customers
# MAGIC   where CustomerNationalRegistry = '94261534000115'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_registration
# MAGIC where BillingCode = '219508'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from gold.fact_volumeliterstlogMonthly
# MAGIC where SK_Registration in ('-7043861431039944367',
# MAGIC '-5880194871038386625',
# MAGIC '-3589949986037568811',
# MAGIC '6704114796494351902',
# MAGIC '7536790730565048711',
# MAGIC '7919354738276690922')

# COMMAND ----------

# DBTITLE 1,query ajustada - crispim
# MAGIC %sql
# MAGIC with cd_fat as (
# MAGIC   select 
# MAGIC     a.Sk_Registration,
# MAGIC     a.Sk_Customers,
# MAGIC     a.ActivationDate as Data_ativacao,
# MAGIC     a.LastTransactionDate as Data_ultima_transacao,
# MAGIC     a.RegistrationStatus as Status
# MAGIC   from gold.dim_registration a 
# MAGIC ),
# MAGIC
# MAGIC conta_depara as (
# MAGIC   select 
# MAGIC     ` CNPJ `,
# MAGIC     regexp_replace(` CNPJ `, '[^0-9]', '') AS CNPJ_LIMPO,
# MAGIC     lpad(regexp_replace(` CNPJ `, '[^0-9]', ''), 14, '0') AS CNPJ_FORMATADO
# MAGIC   from incentivos.pesquisa_ticket_log_cris
# MAGIC ),
# MAGIC
# MAGIC conta as (
# MAGIC   select 
# MAGIC     b.sk_customers,
# MAGIC     b.CustomerNationalRegistry as Cnpj 
# MAGIC   from gold.dim_customers b 
# MAGIC   inner join conta_depara e
# MAGIC     on b.CustomerNationalRegistry = e.CNPJ_FORMATADO
# MAGIC ),
# MAGIC
# MAGIC Volume_liters as (
# MAGIC   select
# MAGIC     c.SK_Registration,
# MAGIC     c.ReferenceDate,
# MAGIC     c.ProductType,
# MAGIC     c.FuelType,
# MAGIC     c.Volume,
# MAGIC     c.Liters
# MAGIC   from gold.fact_volumeliterstlogMonthly c 
# MAGIC   where c.FuelType NOT IN ("Others")
# MAGIC     and c.ReferenceDate >= trunc(add_months(current_date(), -2), 'MM')
# MAGIC ),
# MAGIC
# MAGIC receita_mensal as (
# MAGIC   select
# MAGIC     SK_Registration,
# MAGIC     ReferenceDate,
# MAGIC     sum(RevenueTotal) as Receita
# MAGIC   from gold.fact_revenuetlogmonthly
# MAGIC   where ReferenceDate >= trunc(add_months(current_date(), -2), 'MM')
# MAGIC   group by SK_Registration, ReferenceDate
# MAGIC )
# MAGIC
# MAGIC -- Tabela final
# MAGIC select 
# MAGIC   conta.Cnpj,
# MAGIC   cd_fat.Data_ativacao,
# MAGIC   cd_fat.data_ultima_transacao,
# MAGIC   cd_fat.Status,
# MAGIC   volume_liters.ReferenceDate as Data_referencia,
# MAGIC   volume_liters.ProductType as Produto,
# MAGIC   sum(volume_liters.Volume) as Volume,
# MAGIC   sum(volume_liters.Liters) as Litros,
# MAGIC   max(receita_mensal.Receita) as Receita -- <- Aqui está o segredo
# MAGIC from cd_fat
# MAGIC inner join conta
# MAGIC   on cd_fat.Sk_Customers = conta.sk_customers
# MAGIC inner join Volume_liters
# MAGIC   on cd_fat.Sk_Registration = volume_liters.SK_Registration
# MAGIC left join receita_mensal
# MAGIC   on cd_fat.Sk_Registration = receita_mensal.SK_Registration
# MAGIC  and volume_liters.ReferenceDate = receita_mensal.ReferenceDate
# MAGIC group by 1,2,3,4,5,6
# MAGIC

# COMMAND ----------

# DBTITLE 1,Base crispim - volume Abastecimento
# MAGIC %sql
# MAGIC with cd_fat as (
# MAGIC   select 
# MAGIC   a.Sk_Registration
# MAGIC , a.Sk_Customers
# MAGIC , a.ActivationDate as Data_ativacao
# MAGIC , a.LastTransactionDate as Data_ultima_transacao
# MAGIC , a.RegistrationStatus as Status
# MAGIC
# MAGIC   from gold.dim_registration a 
# MAGIC   --where IsLastVersion = 'true'
# MAGIC )
# MAGIC , conta_depara as (
# MAGIC   --crispim
# MAGIC   select 
# MAGIC   ` CNPJ `
# MAGIC  -- Remove pontuação
# MAGIC   , regexp_replace(` CNPJ `, '[^0-9]', '') AS CNPJ_LIMPO
# MAGIC   , lpad(regexp_replace(` CNPJ `, '[^0-9]', ''), 14, '0') AS CNPJ_FORMATADO
# MAGIC   , length(lpad(regexp_replace(` CNPJ `, '[^0-9]', ''), 14, '0')) AS QTD_CARACTERES
# MAGIC
# MAGIC from incentivos.pesquisa_ticket_log_cris
# MAGIC )
# MAGIC
# MAGIC ,conta as (
# MAGIC select 
# MAGIC   b.sk_customers
# MAGIC , b.CustomerNationalRegistry as Cnpj 
# MAGIC from gold.dim_customers b 
# MAGIC inner join conta_depara e
# MAGIC on b.CustomerNationalRegistry = e.CNPJ_FORMATADO
# MAGIC --where b.CustomerNationalRegistry in (select CNPJ_FORMATADO from conta_depara)
# MAGIC )
# MAGIC
# MAGIC ,Volume_liters as (
# MAGIC   select
# MAGIC   c.SK_Registration
# MAGIC , c.ReferenceDate
# MAGIC , c.ProductType 
# MAGIC , c.FuelType 
# MAGIC , c.Volume  
# MAGIC , c.Liters  
# MAGIC , rev.RevenueTotal    
# MAGIC   from gold.fact_volumeliterstlogMonthly c 
# MAGIC
# MAGIC   inner join gold.fact_revenuetlogmonthly as rev
# MAGIC   on  c.SK_Registration = rev.SK_Registration
# MAGIC   and c.ReferenceDate = rev.ReferenceDate
# MAGIC   --, rev.ReferenceDate
# MAGIC   where c.FuelType NOT IN ("Others") and  c.ReferenceDate >= trunc(add_months(current_date(), -2), 'MM')
# MAGIC )
# MAGIC
# MAGIC -->> Tb final 
# MAGIC select 
# MAGIC   conta.Cnpj
# MAGIC , cd_fat.Data_ativacao as Data_ativacao
# MAGIC , cd_fat.data_ultima_transacao
# MAGIC , cd_fat.Status
# MAGIC -->> colocar o volume dos ultimos 3 meses
# MAGIC , volume_liters.ReferenceDate as Data_referencia
# MAGIC , volume_liters.ProductType as Produto
# MAGIC , sum(volume_liters.Volume) as Volume 
# MAGIC , sum(volume_liters.Liters) as Litros
# MAGIC , sum(volume_liters.RevenueTotal) as Receita
# MAGIC
# MAGIC from cd_fat
# MAGIC
# MAGIC inner join conta
# MAGIC on cd_fat.Sk_Customers = conta.sk_customers
# MAGIC inner join Volume_liters
# MAGIC on cd_fat.Sk_Registration = volume_liters.SK_Registration
# MAGIC
# MAGIC group by 1, 3 --2,3,4,5,6
# MAGIC
# MAGIC     

# COMMAND ----------

# DBTITLE 1,Volume Repom
# MAGIC %sql
# MAGIC with cd_fat as (
# MAGIC   select 
# MAGIC   a.Sk_Registration
# MAGIC , a.Sk_Customers
# MAGIC , a.ActivationDate as Data_ativacao
# MAGIC , a.LastTransactionDate as Data_ultima_transacao
# MAGIC , a.RegistrationStatus as Status
# MAGIC
# MAGIC   from gold.dim_registration a 
# MAGIC   --where IsLastVersion = 'true'
# MAGIC )
# MAGIC , conta_depara as (
# MAGIC   --crispim
# MAGIC   select 
# MAGIC   ` CNPJ `
# MAGIC  -- Remove pontuação
# MAGIC   , regexp_replace(` CNPJ `, '[^0-9]', '') AS CNPJ_LIMPO
# MAGIC   , lpad(regexp_replace(` CNPJ `, '[^0-9]', ''), 14, '0') AS CNPJ_FORMATADO
# MAGIC   , length(lpad(regexp_replace(` CNPJ `, '[^0-9]', ''), 14, '0')) AS QTD_CARACTERES
# MAGIC
# MAGIC from incentivos.pesquisa_ticket_log_cris
# MAGIC )
# MAGIC
# MAGIC ,conta as (
# MAGIC select 
# MAGIC   b.sk_customers
# MAGIC , b.CustomerNationalRegistry as Cnpj 
# MAGIC from gold.dim_customers b 
# MAGIC inner join conta_depara e
# MAGIC on b.CustomerNationalRegistry = e.CNPJ_FORMATADO
# MAGIC --where b.CustomerNationalRegistry in (select CNPJ_FORMATADO from conta_depara)
# MAGIC )
# MAGIC
# MAGIC ,Volume_freight as (
# MAGIC   select
# MAGIC   c.SK_Registration
# MAGIC , date_format(c.IssueDate, 'yyyy-MM-01') as data_emissao   
# MAGIC , c.FreightValue
# MAGIC , c.EmissionAmount
# MAGIC
# MAGIC   from gold.fact_volumefreightdaily c 
# MAGIC   where  date_format(c.IssueDate, 'yyyy-MM-01') >= trunc(add_months(current_date(), -2), 'MM')
# MAGIC )
# MAGIC
# MAGIC -->> Tb final 
# MAGIC select 
# MAGIC   conta.Cnpj
# MAGIC , cd_fat.Data_ativacao
# MAGIC --, cd_fat.data_ultima_transacao
# MAGIC , cd_fat.Status
# MAGIC -->> colocar o volume dos ultimos 3 meses
# MAGIC , Volume_freight.data_emissao 
# MAGIC , round(sum(Volume_freight.FreightValue),2) as FreightValue 
# MAGIC , sum(Volume_freight.EmissionAmount) as EmissionAmount
# MAGIC
# MAGIC from cd_fat
# MAGIC
# MAGIC inner join conta
# MAGIC on cd_fat.Sk_Customers = conta.sk_customers
# MAGIC inner join Volume_freight
# MAGIC on cd_fat.Sk_Registration = Volume_freight.SK_Registration
# MAGIC
# MAGIC group by 1,2,3,4--,5--,6
# MAGIC
# MAGIC     

# COMMAND ----------

# DBTITLE 1,Volume tolls
# MAGIC %sql
# MAGIC with cd_fat as (
# MAGIC   select 
# MAGIC   a.Sk_Registration
# MAGIC , a.Sk_Customers
# MAGIC , a.ActivationDate as Data_ativacao
# MAGIC , a.LastTransactionDate as Data_ultima_transacao
# MAGIC , a.RegistrationStatus as Status
# MAGIC
# MAGIC   from gold.dim_registration a 
# MAGIC   --where IsLastVersion = 'true'
# MAGIC )
# MAGIC , conta_depara as (
# MAGIC   --crispim
# MAGIC   select 
# MAGIC   ` CNPJ `
# MAGIC  -- Remove pontuação
# MAGIC   , regexp_replace(` CNPJ `, '[^0-9]', '') AS CNPJ_LIMPO
# MAGIC   , lpad(regexp_replace(` CNPJ `, '[^0-9]', ''), 14, '0') AS CNPJ_FORMATADO
# MAGIC   , length(lpad(regexp_replace(` CNPJ `, '[^0-9]', ''), 14, '0')) AS QTD_CARACTERES
# MAGIC
# MAGIC from incentivos.pesquisa_ticket_log_cris
# MAGIC )
# MAGIC
# MAGIC ,conta as (
# MAGIC select 
# MAGIC   b.sk_customers
# MAGIC , b.CustomerNationalRegistry as Cnpj 
# MAGIC from gold.dim_customers b 
# MAGIC inner join conta_depara e
# MAGIC on b.CustomerNationalRegistry = e.CNPJ_FORMATADO
# MAGIC --where b.CustomerNationalRegistry in (select CNPJ_FORMATADO from conta_depara)
# MAGIC )
# MAGIC
# MAGIC ,Volume_tolls as (
# MAGIC   select
# MAGIC   c.Sk_Registration
# MAGIC , date_format(c.ReferenceDate, 'yyyy-MM-01') as Data_passagem
# MAGIC , c.AbsoluteAmount as valor_transacionado
# MAGIC , c.TransactedAmount
# MAGIC
# MAGIC   from gold.fact_tollpassages c 
# MAGIC   where  date_format(c.ReferenceDate, 'yyyy-MM-01') >= trunc(add_months(current_date(), -2), 'MM')
# MAGIC )
# MAGIC
# MAGIC -->> Tb final 
# MAGIC select 
# MAGIC   conta.Cnpj
# MAGIC , cd_fat.Data_ativacao
# MAGIC --, cd_fat.data_ultima_transacao
# MAGIC , cd_fat.Status
# MAGIC -->> colocar o volume dos ultimos 3 meses
# MAGIC , Volume_tolls.Data_passagem 
# MAGIC , sum(Volume_tolls.valor_transacionado) as valor_transacionado
# MAGIC --, sum(Volume_tolls.TransactedAmount) as TransactedAmount
# MAGIC
# MAGIC from cd_fat
# MAGIC
# MAGIC inner join conta
# MAGIC on cd_fat.Sk_Customers = conta.sk_customers
# MAGIC inner join Volume_tolls
# MAGIC on cd_fat.Sk_Registration = Volume_tolls.SK_Registration
# MAGIC
# MAGIC group by 1,2,3,4--,5--,6
# MAGIC
# MAGIC     
# MAGIC

# COMMAND ----------

# DBTITLE 1,check volume e receita
# MAGIC %sql
# MAGIC -- Volume_liters as (
# MAGIC   select
# MAGIC   c.SK_Registration  
# MAGIC ,  reg.BillingCode  
# MAGIC , c.ReferenceDate
# MAGIC --, c.ProductType 
# MAGIC --, c.FuelType 
# MAGIC , sum(c.Volume) as volume   
# MAGIC , sum (c.Liters) as litros  
# MAGIC , sum(rev.RevenueTotal) as receita     
# MAGIC   from gold.fact_volumeliterstlogMonthly c 
# MAGIC
# MAGIC   inner join gold.fact_revenuetlogmonthly as rev
# MAGIC   on  c.SK_Registration = rev.SK_Registration
# MAGIC   and c.ReferenceDate = rev.ReferenceDate
# MAGIC
# MAGIC   left join gold.dim_registration as reg
# MAGIC   on c.SK_Registration = reg.Sk_Registration
# MAGIC   --, rev.ReferenceDate
# MAGIC   where c.FuelType NOT IN ("Others") and BillingCode = '219508' --and  c.ReferenceDate >= trunc(add_months(current_date(), -2), 'MM')
# MAGIC   group by 1,2,3,4,5,6
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_registration

# COMMAND ----------

# DBTITLE 1,Receita Hits
# MAGIC %sql 
# MAGIC
# MAGIC with tab_TRANSACAO as (
# MAGIC     select tr.ContratoId
# MAGIC          , year(tr.DataAutorizacao) as nr_ano_referencia
# MAGIC          , month(tr.DataAutorizacao) as nr_mes_referencia
# MAGIC          , sum(case when tr.TipoTransacaoId = 'FIRST.PRES-PURCHASE.ORIG'  then tr.ValorTransacao else 0 end) as vl_autorizado_tr
# MAGIC          , sum(case when tr.TipoTransacaoId = 'PGTO.BOLETO'  then tr.ValorTransacao else 0 end) as vl_autorizado_boleto
# MAGIC          , sum(case when tr.TipoTransacaoId = 'PGTO.BOLETO.RVSL'  then tr.ValorTransacao else 0 end) as vl_autorizado_boleto_rvs
# MAGIC          , (vl_autorizado_tr + vl_autorizado_boleto - vl_autorizado_boleto_rvs) as vl_autorizado
# MAGIC          , sum(case when tr.TipoTransacaoId = 'FIRST.PRES-PURCHASE.ORIG' and tr.Bandeira='INCOMMIG_ELO'  then tr.ValorTransacao else 0 end) as vl_autorizado_tr_elo
# MAGIC          , vl_autorizado_tr_elo * 0.0076 as vl_receita_rede_elo
# MAGIC          
# MAGIC     from bronze.hits_pay_transacoes as tr
# MAGIC     group by tr.ContratoId, year(tr.DataAutorizacao), month(tr.DataAutorizacao)
# MAGIC ),
# MAGIC tab_faturamento as (
# MAGIC     select fat.Contrato
# MAGIC          , year(fat.DataPagamentoNota) as nr_ano_referencia
# MAGIC          , month(fat.DataPagamentoNota) as nr_mes_referencia
# MAGIC          , sum(fat.Volume) as vl_recarga
# MAGIC          , COALESCE(sum(fat.Comissao), 0) + COALESCE(sum(fat.Tarifas), 0) as vl_receita_cliente
# MAGIC     from bronze.hits_pay_faturamento as fat
# MAGIC     where fat.DataPagamentoNota is not null -- recargas pagas
# MAGIC     group by fat.Contrato, year(fat.DataPagamentoNota), month(fat.DataPagamentoNota)
# MAGIC ),
# MAGIC
# MAGIC tab_cobrand as (
# MAGIC           select pay_cart.ContratoId as ContratoId
# MAGIC           , year(tr.dt_transacao) as nr_ano_referencia
# MAGIC           , month(tr.dt_transacao) as nr_mes_referencia
# MAGIC           , sum(tr.vl_transacao) as vl_transacao_goodcard
# MAGIC           , sum(tax.vl_comissao) as vl_receita_goodcard
# MAGIC
# MAGIC           from bronze.tlbagda_cobrand_transacao_cobrand as tr
# MAGIC
# MAGIC           left join bronze.tlbagda_cobrand_transacao_parcela_cobrand as tax
# MAGIC           on tr.cd_transacao_cobrand = tax.cd_transacao_cobrand
# MAGIC
# MAGIC           inner join bronze.hits_pay_cartao as pay_cart
# MAGIC           on tr.nr_cartao = pay_cart.NumeroCartao
# MAGIC
# MAGIC           where 1=1
# MAGIC           AND tr.cd_status_transacao = 2 -- transação validada
# MAGIC           AND tr.cd_emissor_cartao = 20 -- EDENRED SOLUCOES DE PAGAMENTOS HYLA S.A.
# MAGIC           AND date_format(STRING(dt_transacao), 'yyyy-MM-dd') >= "2023-09-01"
# MAGIC           group by pay_cart.ContratoId, year(tr.dt_transacao), month(tr.dt_transacao)
# MAGIC ),
# MAGIC
# MAGIC combined as (
# MAGIC     select coalesce(t.ContratoId, f.Contrato) as Contrato
# MAGIC          , coalesce(t.nr_ano_referencia, f.nr_ano_referencia, c.nr_ano_referencia) as nr_ano_referencia
# MAGIC          , coalesce(t.nr_mes_referencia, f.nr_mes_referencia, c.nr_mes_referencia) as nr_mes_referencia
# MAGIC          , coalesce(t.vl_autorizado, 0) as vl_autorizado
# MAGIC          , coalesce(f.vl_recarga, 0) as vl_recarga
# MAGIC          , coalesce(f.vl_receita_cliente, 0) as vl_receita_cliente
# MAGIC          , coalesce(t.vl_receita_rede_elo, 0) as vl_receita_rede_elo
# MAGIC          , coalesce(c.vl_receita_goodcard, 0) as vl_receita_goodcard
# MAGIC          , coalesce(t.vl_receita_rede_elo, 0) + coalesce(c.vl_receita_goodcard, 0) as vl_receita_rede
# MAGIC     from tab_TRANSACAO t
# MAGIC     full outer join tab_faturamento f
# MAGIC     on t.ContratoId = f.Contrato
# MAGIC     and t.nr_ano_referencia = f.nr_ano_referencia
# MAGIC     and t.nr_mes_referencia = f.nr_mes_referencia
# MAGIC     full outer join tab_cobrand c
# MAGIC     on t.ContratoId = c.ContratoId
# MAGIC     and t.nr_ano_referencia = c.nr_ano_referencia
# MAGIC     and t.nr_mes_referencia = c.nr_mes_referencia
# MAGIC )
# MAGIC
# MAGIC select cd_fat.Id as id_codigo_faturamento
# MAGIC      , cd_fat.Contrato_Iflex__c as nr_contrato_iflex
# MAGIC      , cd_fat.Name as codigo_faturamento
# MAGIC      , cd_fat.Tipo__c as tipo_codigo_faturamento
# MAGIC      , c.nr_ano_referencia as nr_ano_referencia
# MAGIC      , c.nr_mes_referencia as nr_mes_referencia
# MAGIC      , c.vl_autorizado as vl_autorizado
# MAGIC      , c.vl_recarga as vl_recarga
# MAGIC      , c.vl_receita_cliente as vl_receita_cliente
# MAGIC      , c.vl_receita_rede as vl_receita_rede
# MAGIC
# MAGIC from bronze.salesforce_salesforce_codigofaturamento__c as cd_fat
# MAGIC
# MAGIC inner join bronze.hits_pay_contrato as pay_contrato
# MAGIC on cd_fat.Contrato_Iflex__c = pay_contrato.NumeroContrato
# MAGIC inner join combined c
# MAGIC on pay_contrato.NumeroContrato = c.Contrato
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Receita abastecimento
# MAGIC %sql
# MAGIC --Receita 
# MAGIC select cust.BillingCode
# MAGIC , rev.ReferenceDate
# MAGIC , rev.ProductName
# MAGIC , rev.RevenueTotal
# MAGIC , rev.RevenueEstablishment
# MAGIC , rev.RevenueClientTotal
# MAGIC
# MAGIC from gold.fact_revenuetlogmonthly as rev
# MAGIC
# MAGIC left join gold.dim_registration as cust
# MAGIC on rev.SK_Registration = cust.Sk_Registration
# MAGIC
# MAGIC where 1=1
# MAGIC AND date_format(STRING(rev.ReferenceDate), 'yyyy-MM-dd')>= "2023-07-01"
# MAGIC and rev.ProductName in ('Abastecimento')

# COMMAND ----------

# DBTITLE 1,Litragem & Volume
# MAGIC %sql
# MAGIC -- Volume && Litros - MGM 
# MAGIC
# MAGIC SELECT tr.*
# MAGIC , cust.BillingCode
# MAGIC
# MAGIC FROM gold.fact_volumeliterstlogMonthly AS tr
# MAGIC
# MAGIC LEFT JOIN gold.dim_registration AS cust
# MAGIC ON tr.SK_Registration = cust.SK_Registration
# MAGIC
# MAGIC --Tabela de receita abastecimento
# MAGIC /*LEFT JOIN (
# MAGIC     select cust.BillingCode
# MAGIC     , rev.ReferenceDate
# MAGIC     , rev.ProductName
# MAGIC     , rev.RevenueTotal
# MAGIC     , rev.RevenueEstablishment
# MAGIC     , rev.RevenueClientTotal
# MAGIC
# MAGIC     from gold.fact_revenuetlogmonthly as rev
# MAGIC
# MAGIC     left join gold.dim_registration as cust
# MAGIC     on rev.SK_Registration = cust.Sk_Registration
# MAGIC
# MAGIC     where 1=1
# MAGIC     AND date_format(STRING(rev.ReferenceDate), 'yyyy-MM-dd')>= "2023-07-01"
# MAGIC     and rev.ProductName in ('Abastecimento')
# MAGIC ) as receita
# MAGIC on cust.BillingCode =  receita.BillingCode*/
# MAGIC
# MAGIC -- Inner Join com a tabela de código de faturamento
# MAGIC INNER JOIN (
# MAGIC     SELECT DISTINCT cd_fat.Name AS cd_faturamento
# MAGIC
# MAGIC     FROM bronze.salesforce_salesforce_codigofaturamento__c AS cd_fat
# MAGIC
# MAGIC     LEFT JOIN bronze.salesforce_salesforce_account AS acc
# MAGIC     ON cd_fat.Conta__c = acc.Id
# MAGIC
# MAGIC     WHERE (
# MAGIC         cd_fat.OrigemLead__c IN ("Member Get Member", "Member Get Member Qualificado")
# MAGIC         OR cd_fat.NomeOrigem__c IN ("beeviral", "loja-mgm")
# MAGIC         OR cd_fat.CupomECommerce__c = "beeviral"
# MAGIC         OR cd_fat.Name IN ('356121', '356123', '356124', '356145', '356147', '356169', '356182', '356209', '356218', '355999', '356001', '356159', '356161', '356178', '356188', '356196', '356201','234892', '232670')
# MAGIC     )
# MAGIC     AND date_format(STRING(cd_fat.CreatedDate), 'yyyy-MM-dd') >= "2023-07-01"
# MAGIC     AND acc.CNPJ_2__c NOT IN ("02147557000266")
# MAGIC ) AS faturamento
# MAGIC ON cust.BillingCode = faturamento.cd_faturamento  
# MAGIC
# MAGIC WHERE 1=1
# MAGIC AND date_format(STRING(tr.ReferenceDate), 'yyyy-MM-dd') >= "2023-07-01"
# MAGIC AND tr.FuelType NOT IN ("Others")
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cd_Fat - MGM
# MAGIC %sql
# MAGIC
# MAGIC -- Códigos de faturamento da área
# MAGIC
# MAGIC select cd_fat.Id
# MAGIC , cd_fat.CreatedDate as Data_Criacao
# MAGIC , cd_fat.DataAtivacao__c as Data_ativacao
# MAGIC , cd_fat.DataUltimaTransacao__c as ultima_transacao
# MAGIC --, date_format(STRING(cd_fat.CreatedDate), 'dd/MM/yyyy') as Data_Criacao
# MAGIC , case when (cd_fat.OrigemLead__c = "Member Get Member" or cd_fat.OrigemLead__c = "Member Get Member Qualificado") then "Indicação Direta"
# MAGIC        when (cd_fat.OrigemLead__c = "Online Sales" or cd_fat.NomeOrigem__c = "beeviral") then "Loja - Autonomist"  
# MAGIC        when cd_fat.NomeOrigem__c = "loja-mgm" then "Loja - Abandonist" else cd_fat.OrigemLead__c end as Tipo_Indicacao
# MAGIC
# MAGIC , case when cd_fat.OrigemLead__c = "Member Get Member" then cd_fat.NomeOrigem__c
# MAGIC        when cd_fat.OrigemLead__c = "Online Sales" then cd_fat.GoogleAnalyticsCampaign__c 
# MAGIC        when cd_fat.NomeOrigem__c = "loja-mgm" then cd_fat.GoogleAnalyticsCampaign__c else null end as Campanha
# MAGIC , cd_fat.Name as cd_faturamento
# MAGIC , CASE 
# MAGIC     WHEN cd_fat.PerfilProduto__c IS NULL OR cd_fat.PerfilProduto__c = '' 
# MAGIC     THEN cd_fat.TipoRegistroProduto__c
# MAGIC     ELSE cd_fat.PerfilProduto__c
# MAGIC   END AS Perfil_Produto
# MAGIC , acc.CNPJ_2__c as CNPJ
# MAGIC , acc.RazaoSocial__c as RazaoSocial
# MAGIC ,cd_fat.Status__c
# MAGIC , rf.MainEconomicActivityCode
# MAGIC , rf.CompanySizeCode
# MAGIC ,CASE 
# MAGIC           WHEN 
# MAGIC               rf.CompanySizeCode = 'Micro' AND(
# MAGIC               REGEXP_LIKE(SUBSTRING(acc.RazaoSocial__c, 1, 2), '^[0-9]+$') OR 
# MAGIC               REGEXP_LIKE(SUBSTRING(acc.RazaoSocial__c, -2), '^[0-9]+$') )
# MAGIC           THEN 'MEI' 
# MAGIC           ELSE 'não MEI' 
# MAGIC         END AS check_MEI
# MAGIC         ,CASE 
# MAGIC           WHEN rf.CompanySizeCode <> 'Micro' THEN rf.CompanySizeCode
# MAGIC           WHEN rf.CompanySizeCode = 'Micro' AND check_MEI = 'MEI' THEN 'MEI'
# MAGIC           WHEN rf.CompanySizeCode = 'Micro' AND check_MEI = 'não MEI' THEN 'ME'
# MAGIC         END AS Porte_Final
# MAGIC , cd_fat.OrigemLead__c as Origem_Lead
# MAGIC , cd_fat.NomeOrigem__c as Nome_Origem
# MAGIC , cd_fat.CupomECommerce__c as cupom_ecommerce
# MAGIC ,case 
# MAGIC       when cd_fat.CartoesEmbossados__c IS NULL OR cd_fat.CartoesEmbossados__c = '' 
# MAGIC       THEN cd_fat.CartoesGerados__c
# MAGIC       ELSE cd_fat.CartoesEmbossados__c
# MAGIC     END AS Cartoes_embossados
# MAGIC , cd_fat.CartoesAtivos__c as Cartoes_ativos
# MAGIC , cd_fat.CartoesAtivosMes__c as cartoes_ativos_mes
# MAGIC , cd_fat.GoogleAnalyticsCampaign__c
# MAGIC , cd_fat.GoogleAnalyticsContent__c
# MAGIC , cd_fat.GoogleAnalyticsMedium__c
# MAGIC
# MAGIC from bronze.salesforce_salesforce_codigofaturamento__c as cd_fat
# MAGIC left join bronze.salesforce_salesforce_account as acc
# MAGIC on cd_fat.Conta__c = acc.Id
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC     gold.Dim_FederalRevenueServices AS rf
# MAGIC     ON acc.CNPJ_2__c = rf.CustomerNationalRegistry
# MAGIC
# MAGIC where 1=1
# MAGIC and (
# MAGIC     cd_fat.OrigemLead__c in ("Member Get Member",'Member Get Member Qualificado')
# MAGIC     or cd_fat.NomeOrigem__c IN ('beeviral', 'loja-mgm') 
# MAGIC     or cd_fat.CupomECommerce__c = "beeviral"
# MAGIC     or cd_fat.Name in ('356121', '356123', '356124', '356145', '356147', '356169', '356182', '356209', '356218', '355999', '356001', '356159', '356161', '356178', '356188', '356196', '356201','234892', '232670')
# MAGIC )
# MAGIC AND date_format(STRING(cd_fat.CreatedDate), 'yyyy-MM-dd') >= "2023-07-01"
# MAGIC and acc.CNPJ_2__c not in ("02147557000266")
# MAGIC and cd_fat.Id not in ("a0OSG000006aaW92AI")
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Receita Federal - CNPJs ativo por Subclasse/CNAE
# MAGIC %sql
# MAGIC
# MAGIC select rf.MainEconomicActivityCode
# MAGIC , count(rf.CustomerNationalRegistry)
# MAGIC
# MAGIC from gold.dim_federalrevenueservices as rf
# MAGIC
# MAGIC where 1=1 
# MAGIC and rf.RegistrationStatus = "Active"
# MAGIC
# MAGIC group by rf.MainEconomicActivityCode
# MAGIC --limit 1000

# COMMAND ----------

# DBTITLE 1,Leads - MGM
# MAGIC %sql 
# MAGIC --Leads
# MAGIC SELECT 
# MAGIC   lead.UnidadeNegocios__c,
# MAGIC   date_format(lead.DataCriacao__c, 'yyyy-MM-dd HH:mm:ss') as data_criacao,
# MAGIC   lead.FirstName,
# MAGIC   lead.Email, 
# MAGIC   lead.RazaoSocial__c,
# MAGIC   lead.Company,
# MAGIC   lead.cnpj__c, 
# MAGIC   lead.cnpj_2__c, 
# MAGIC   lead.Status, 
# MAGIC   lead.MotivoNaoQualificar__c,
# MAGIC   lead.Motivo__c,
# MAGIC   lead.leadsource, 
# MAGIC   lead.NomeOrigem__c,
# MAGIC   lead.UTM_Campaign__c,
# MAGIC   lead.UTM_Medium__c,
# MAGIC   lead.DataFinalizacao__c,
# MAGIC   lead.Id,
# MAGIC   rf.MainEconomicActivityCode
# MAGIC , rf.CompanySizeCode
# MAGIC ,CASE 
# MAGIC           WHEN 
# MAGIC               rf.CompanySizeCode = 'Micro' AND(
# MAGIC               REGEXP_LIKE(SUBSTRING(lead.RazaoSocial__c, 1, 2), '^[0-9]+$') OR 
# MAGIC               REGEXP_LIKE(SUBSTRING(lead.RazaoSocial__c, -2), '^[0-9]+$') )
# MAGIC           THEN 'MEI' 
# MAGIC           ELSE 'não MEI' 
# MAGIC         END AS check_MEI
# MAGIC         ,CASE 
# MAGIC           WHEN rf.CompanySizeCode <> 'Micro' THEN rf.CompanySizeCode
# MAGIC           WHEN rf.CompanySizeCode = 'Micro' AND check_MEI = 'MEI' THEN 'MEI'
# MAGIC           WHEN rf.CompanySizeCode = 'Micro' AND check_MEI = 'não MEI' THEN 'ME'
# MAGIC         END AS Porte_Final
# MAGIC
# MAGIC FROM bronze.salesforce_salesforce_lead AS lead
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC     gold.Dim_FederalRevenueServices AS rf
# MAGIC     ON lead.cnpj_2__c = rf.CustomerNationalRegistry
# MAGIC
# MAGIC WHERE lead.DataCriacao__c >= '2023-07-01'
# MAGIC AND (lead.LeadSource IN ('Member Get Member','Member Get Member Qualificado') OR lead.NomeOrigem__c IN ('beeviral', 'loja-mgm'))
# MAGIC
# MAGIC
# MAGIC
# MAGIC  
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Comunicação - MGM - Infobip
# MAGIC %sql
# MAGIC --Envios Infobip
# MAGIC
# MAGIC SELECT 
# MAGIC     infobip.CommunicationName AS NomeComunicacao,
# MAGIC     COALESCE(infobip.CommunicationStartTimeStamp) AS CommunicationStartDate,
# MAGIC     infobip.ServiceName,
# MAGIC     SUM(COALESCE(infobip.MessagesCount, 0)) AS TotalMessagesCount,
# MAGIC     SUM(COALESCE(CASE WHEN infobip.NameStatus = 'Delivered' THEN 1 ELSE 0 END, 0)) AS DeliveredCount,
# MAGIC     (COALESCE(SUM(CASE WHEN infobip.NameStatus = 'Delivered' THEN 1 ELSE 0 END), 0) / COALESCE(SUM(infobip.MessagesCount), 1)) AS DeliveredPercentual,
# MAGIC     SUM(COALESCE(CASE WHEN infobip.NameStatus = 'Rejected' THEN 1 ELSE 0 END, 0)) AS RejectedCount,
# MAGIC     SUM(COALESCE(CASE WHEN infobip.NameStatus = 'Undeliverable' THEN 1 ELSE 0 END, 0)) AS UndeliverableCount,
# MAGIC     SUM(COALESCE(CASE WHEN infobip.NameStatus = 'Expired' THEN 1 ELSE 0 END, 0)) AS ExpiredCount,
# MAGIC     SUM(COALESCE(infobip.Opens, 0)) AS TotalOpens,
# MAGIC     (COALESCE(SUM(infobip.Opens), 0) / COALESCE(SUM(CASE WHEN infobip.NameStatus = 'Delivered' THEN 1 ELSE 0 END), 1)) AS OpensPercentual,
# MAGIC     SUM(COALESCE(infobip.Clicks, 0)) AS TotalClicks,
# MAGIC     (COALESCE(SUM(infobip.Clicks), 0) / COALESCE(SUM(infobip.Opens), 1)) AS ClicksPercentual,
# MAGIC     COUNT(CASE WHEN infobip.SeenAtTimestamp IS NOT NULL AND ServiceName = 'WhatsApp Outbound' THEN 1 ELSE NULL END) AS TotalVisualizacoes, 
# MAGIC     CASE
# MAGIC         WHEN LOWER(infobip.CommunicationName) RLIKE 'usu[áa]rios' THEN 'Indique e ganhe Edenred Ticket Log'
# MAGIC         WHEN lower(infobip.CommunicationName) LIKE '%clientes%' 
# MAGIC         OR lower(infobip.CommunicationName) LIKE '%beeviraloficial%' 
# MAGIC         THEN 'Indique e ganhe TicketLog Oficial'
# MAGIC         WHEN LOWER(REPLACE(infobip.CommunicationName, ' ', '')) LIKE '%parceirohits%' THEN 'Parceiro Hits'
# MAGIC         WHEN lower(infobip.CommunicationName) LIKE '%canais%' THEN 'Canais parceiro Ticket Log'
# MAGIC         WHEN lower(infobip.CommunicationName) LIKE '%teste%'  THEN 'Teste'
# MAGIC         WHEN LOWER(infobip.CommunicationName) LIKE '%mgmhits%' 
# MAGIC         OR LOWER(infobip.CommunicationName) LIKE '%edenredhits%' 
# MAGIC         OR LOWER(infobip.CommunicationName) LIKE '%beeviralhits%' 
# MAGIC         THEN 'Indique e ganhe Edenred Hits'
# MAGIC         ELSE 'Outros'
# MAGIC     END AS Campanha
# MAGIC
# MAGIC FROM 
# MAGIC     gold.dm_infobip AS infobip
# MAGIC
# MAGIC GROUP BY 
# MAGIC     infobip.communicationName,
# MAGIC     COALESCE(infobip.CommunicationStartTimeStamp),
# MAGIC     infobip.ServiceName
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Comunicação - Genesy
# MAGIC %sql 
# MAGIC
# MAGIC SELECT 
# MAGIC     gen.idcall,
# MAGIC     gen.inicio,
# MAGIC     gen.fim,
# MAGIC     gen.DATA_REFERENCIA,
# MAGIC     gen.duracao,
# MAGIC     gen.resultado,
# MAGIC     gen.agente_login,
# MAGIC     -- Convertendo duração para segundos
# MAGIC     CAST(SUBSTRING(duracao, 12, 2) AS INT) * 3600 +  -- Horas
# MAGIC     CAST(SUBSTRING(duracao, 15, 2) AS INT) * 60 +    -- Minutos
# MAGIC     CAST(SUBSTRING(duracao, 18, 2) AS INT) +         -- Segundos
# MAGIC     CAST(SUBSTRING(duracao, 21, 3) AS DOUBLE) / 1000 AS duracao_segundos,
# MAGIC     
# MAGIC     -- Formatando para MM:SS
# MAGIC     LPAD(CAST(FLOOR(duracao_segundos / 60) AS STRING), 2, '0') || ':' || 
# MAGIC     LPAD(CAST(MOD(duracao_segundos, 60) AS STRING), 2, '0') AS duracao_formatada
# MAGIC
# MAGIC FROM bronze.bezerro_alctel_bi_chamadasoutbound AS gen
# MAGIC
# MAGIC
# MAGIC WHERE AGENTE_LOGIN = 'BUENO Jessica Santos Tavares'
# MAGIC AND INICIO >= '2024-07-01';
# MAGIC

# COMMAND ----------

# DBTITLE 1,Oportunidade - MGM
# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC oport.LeadSource
# MAGIC ,oport.Cnpj__c
# MAGIC ,REGEXP_REPLACE(oport.Cnpj__c, '[^0-9]', '') AS Cnpj_Sem_Pontuacao
# MAGIC ,oport.Name as nome_oportunidade
# MAGIC ,oport.NomeContaOportunidade__c as razao_social
# MAGIC ,oport.NomeProprietario__c
# MAGIC ,oport.CodigoCliente__c
# MAGIC ,oport.CodigoFaturamento__c
# MAGIC ,oport.StageName as fase
# MAGIC ,date_format(oport.CreatedDate, 'yyyy-MM-dd HH:mm:ss') as data_criacao
# MAGIC ,oport.DataUltimaMudancaFase__c
# MAGIC ,oport.PrevisaoMensalFaturamento__c
# MAGIC ,oport.Motivo__c
# MAGIC ,oport.MotivoDaPerda__c
# MAGIC ,oport.JustificativaPerda__c
# MAGIC ,oport.Contato__c
# MAGIC ,oport.PrevisaoNumeroVeiculos__c
# MAGIC ,oport.QuantidadeVeiculos__c
# MAGIC ,oport.NomeOrigem__c
# MAGIC ,oport.GoogleAnalyticsCampaign__c
# MAGIC ,oport.GoogleAnalyticsMedium__c
# MAGIC ,oport.Id
# MAGIC ,oport.CupomECommerce__c
# MAGIC ,rf.MainEconomicActivityCode
# MAGIC , rf.CompanySizeCode
# MAGIC ,CASE 
# MAGIC           WHEN 
# MAGIC               rf.CompanySizeCode = 'Micro' AND(
# MAGIC               REGEXP_LIKE(SUBSTRING(oport.NomeContaOportunidade__c, 1, 2), '^[0-9]+$') OR 
# MAGIC               REGEXP_LIKE(SUBSTRING(oport.NomeContaOportunidade__c, -2), '^[0-9]+$') )
# MAGIC           THEN 'MEI' 
# MAGIC           ELSE 'não MEI' 
# MAGIC         END AS check_MEI
# MAGIC         ,CASE 
# MAGIC           WHEN rf.CompanySizeCode <> 'Micro' THEN rf.CompanySizeCode
# MAGIC           WHEN rf.CompanySizeCode = 'Micro' AND check_MEI = 'MEI' THEN 'MEI'
# MAGIC           WHEN rf.CompanySizeCode = 'Micro' AND check_MEI = 'não MEI' THEN 'ME'
# MAGIC         END AS Porte_Final
# MAGIC
# MAGIC from bronze.salesforce_salesforce_opportunity as oport
# MAGIC
# MAGIC left join
# MAGIC       gold.Dim_FederalRevenueServices AS rf
# MAGIC        ON REPLACE(REPLACE(REPLACE(oport.Cnpj__c, '.', ''), '/', ''),'-','') = rf.CustomerNationalRegistry
# MAGIC
# MAGIC where 
# MAGIC   oport.CreatedDate >= '2023-09-01'
# MAGIC   and oport.LeadSource = 'Member Get Member'
# MAGIC   OR (
# MAGIC         oport.NomeOrigem__c LIKE '%beeviral%' 
# MAGIC         OR oport.NomeOrigem__c LIKE '%loja-mgm%'
# MAGIC         OR oport.NomeOrigem__c = 'beeviral' 
# MAGIC         OR oport.CupomECommerce__c = 'beeviral' 
# MAGIC         OR oport.Id = "0063m00000w31mSAAQ"
# MAGIC         OR oport.Id = "006SG000005tqMXYAY"
# MAGIC         OR oport.Id = "006SG000007HF3UYAW"
# MAGIC         OR oport.Id = "006SG00000G1YWIYA3"
# MAGIC         OR oport.Id = "006SG00000K49KaYAJ"
# MAGIC   )
# MAGIC

# COMMAND ----------

# DBTITLE 1,CD Faturamento Hits com Receita Federal + Neoway
# MAGIC %sql 
# MAGIC
# MAGIC --Cd faturamento autonomist
# MAGIC WITH ranked_contracts AS (
# MAGIC     SELECT 
# MAGIC         cd_fat.Id as Id_faturamento,
# MAGIC         cd_fat.Name as cd_fat,
# MAGIC         cd_fat.Contrato_Iflex__c,
# MAGIC         acc.Cnpj__c,
# MAGIC         acc.CNPJ_2__c,
# MAGIC         acc.RazaoSocial__c,
# MAGIC         cd_fat.UnidadeNegocios__c,
# MAGIC         date_format(cd_fat.CreatedDate, 'yyyy-MM-dd HH:mm:ss') AS data_criacao,
# MAGIC         cd_fat.OrigemLead__c as Origem_Lead,
# MAGIC         cd_fat.NomeOrigem__c as Nome_Origem,
# MAGIC         case when (cd_fat.OrigemLead__c LIKE '%Online Sales%'or cd_fat.OrigemLead__c LIKE '%loja%') then "OLS" else "OUTRO" end as Lead_OLS,
# MAGIC         cd_fat.EstruturaVendas__c,
# MAGIC         cd_fat.EstruturaRelacionamento__c,
# MAGIC         cd_fat.DataAtivacao__c,
# MAGIC         cd_fat.DataMigracaoBaseCarteira__c,
# MAGIC         cd_fat.Status__c,
# MAGIC         cd_fat.CartoesEmbossados__c,
# MAGIC         cd_fat.CartoesAtivos__c,
# MAGIC         cd_fat.PerfilProduto__c,
# MAGIC         cd_fat.Diassemtransacao__c,
# MAGIC         cd_fat.ConsultorRelacionamento__c as Id_consultor, 
# MAGIC         rf_2.SocialCapital,
# MAGIC         rf_2.CompanySizeCode,
# MAGIC         CASE 
# MAGIC           WHEN 
# MAGIC               rf_2.CompanySizeCode = 'Micro' AND(
# MAGIC               REGEXP_LIKE(SUBSTRING(cd_fat.NomeConta__c, 1, 2), '^[0-9]+$') OR 
# MAGIC               REGEXP_LIKE(SUBSTRING(cd_fat.NomeConta__c, -2), '^[0-9]+$') )
# MAGIC           THEN 'MEI' 
# MAGIC           ELSE 'não MEI' 
# MAGIC         END AS check_MEI,
# MAGIC         CASE 
# MAGIC           WHEN rf_2.CompanySizeCode <> 'Micro' THEN rf_2.CompanySizeCode
# MAGIC           WHEN rf_2.CompanySizeCode = 'Micro' AND check_MEI = 'MEI' THEN 'MEI'
# MAGIC           WHEN rf_2.CompanySizeCode = 'Micro' AND check_MEI = 'não MEI' THEN 'ME'
# MAGIC         END AS Porte_Final,
# MAGIC         rf_2.MainEconomicActivityCode,
# MAGIC         rf_2.FederativeUnit,
# MAGIC         rf_2.ActivityStartDate,
# MAGIC         user.name as consultor_responsavel,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY cd_fat.Contrato_Iflex__c ORDER BY cd_fat.CreatedDate DESC) AS row_num
# MAGIC
# MAGIC     FROM bronze.salesforce_salesforce_codigofaturamento__c AS cd_fat
# MAGIC
# MAGIC     LEFT JOIN bronze.salesforce_salesforce_account AS acc
# MAGIC         ON cd_fat.Conta__c = acc.Id
# MAGIC
# MAGIC     LEFT JOIN bronze.salesforce_salesforce_user AS user
# MAGIC     on cd_fat.ConsultorRelacionamento__c = user.Id
# MAGIC
# MAGIC     LEFT JOIN (
# MAGIC         SELECT 
# MAGIC             rf.CustomerNationalRegistry,
# MAGIC             rf.SocialCapital,
# MAGIC             rf.CompanySizeCode,
# MAGIC             rf.MainEconomicActivityCode,
# MAGIC             rf.FederativeUnit,
# MAGIC             rf.ActivityStartDate
# MAGIC         FROM gold.Dim_FederalRevenueServices AS rf
# MAGIC     ) AS rf_2
# MAGIC         ON acc.CNPJ_2__c = rf_2.CustomerNationalRegistry
# MAGIC
# MAGIC     LEFT JOIN (
# MAGIC       SELECT
# MAGIC       b.CNPJ,
# MAGIC       b.Data_Abertura,
# MAGIC       b.Quantidade_Funcionarios_Grupo,
# MAGIC       b.Faturamento_Estimado_Grupo
# MAGIC       FROM bronze.outbound_lead_generation_files AS b
# MAGIC     ) AS neoway
# MAGIC     ON acc.CNPJ_2__c = neoway.CNPJ
# MAGIC
# MAGIC
# MAGIC     inner join bronze.hits_pay_contrato as pay_contrato
# MAGIC     on cd_fat.Contrato_Iflex__c = pay_contrato.NumeroContrato
# MAGIC
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM ranked_contracts
# MAGIC WHERE row_num = 1
# MAGIC ORDER BY Contrato_Iflex__c
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * 
# MAGIC
# MAGIC from bronze.salesforce_salesforce_user
# MAGIC
# MAGIC limit 1000
