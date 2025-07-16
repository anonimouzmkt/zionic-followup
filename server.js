/**
 * ===============================================
 * ZIONIC FOLLOW-UP SERVER
 * ===============================================
 * Servidor automático para reativação de leads inativos
 * 
 * Funcionalidades:
 * - Busca follow-ups pendentes do banco
 * - Verifica contexto da conversa
 * - Gera mensagens personalizadas com IA
 * - Envia via WhatsApp
 * - Registra logs e métricas
 * ✅ CONTROLE AUTOMÁTICO DE CRÉDITOS
 * 
 * Deploy: Render.com
 * Frequência: A cada 2 minutos
 * 
 * @author Zionic Team
 * @version 1.1.0
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const cron = require('node-cron');

// ===============================================
// CONFIGURAÇÕES E INICIALIZAÇÃO
// ===============================================

console.log('🚀 === ZIONIC FOLLOW-UP SERVER INICIANDO ===');
console.log('📅 Timestamp:', new Date().toISOString());
console.log('🌍 Environment:', process.env.NODE_ENV || 'development');

// Configurar Supabase
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ ERRO: Variáveis de ambiente do Supabase não configuradas');
  console.error('Necessário: SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Configurações globais
const CONFIG = {
  maxFollowUpsPerExecution: 50,
  executionIntervalMinutes: 1, // ✅ ATUALIZADO: 1 minuto para maior precisão
  openaiMaxRetries: 3,
  whatsappMaxRetries: 2,
  defaultResponseTimeoutMs: 30000,
  businessHours: {
    start: 8,  // 8h
    end: 18,   // 18h
    timezone: 'America/Sao_Paulo'
  },
  // ✅ NOVO: Configurações de créditos
  credits: {
    estimatedTokensPerFollowUp: 200, // Estimativa de tokens por follow-up
    minimumBalanceThreshold: 1000,   // Mínimo de créditos para funcionar
    tokensToCreditsRatio: 1          // 1 token = 1 crédito
  }
};

// Estatísticas de execução
let stats = {
  totalExecutions: 0,
  totalFollowUpsSent: 0,
  totalOrphansCreated: 0, // ✅ NOVO: Contador de órfãos criados
  totalErrors: 0,
  lastExecution: null,
  serverStartTime: new Date(),
  successRate: 0
};

// ===============================================
// FUNÇÕES UTILITÁRIAS
// ===============================================

/**
 * Log estruturado com timestamp
 */
function log(level, message, data = {}) {
  const timestamp = new Date().toISOString();
  const logEntry = {
    timestamp,
    level,
    message,
    ...data
  };
  
  const emoji = {
    info: 'ℹ️',
    success: '✅',
    warning: '⚠️',
    error: '❌',
    debug: '🔍'
  };
  
  console.log(`${emoji[level] || '📝'} [${timestamp}] ${message}`, 
    Object.keys(data).length > 0 ? JSON.stringify(data, null, 2) : '');
}

/**
 * Verifica se está dentro do horário comercial
 */
function isBusinessHours() {
  const now = new Date();
  const hour = now.getHours();
  return hour >= CONFIG.businessHours.start && hour < CONFIG.businessHours.end;
}

/**
 * Formatar duração em formato legível
 */
function formatDuration(milliseconds) {
  const seconds = Math.floor(milliseconds / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  
  if (hours > 0) return `${hours}h ${minutes % 60}min`;
  if (minutes > 0) return `${minutes}min ${seconds % 60}s`;
  return `${seconds}s`;
}

// ===============================================
// CORE: BUSCAR FOLLOW-UPS PENDENTES
// ===============================================

/**
 * Busca follow-ups prontos para execução
 */
async function getPendingFollowUps() {
  try {
    log('info', 'Buscando follow-ups pendentes...');
    
    const { data: followUps, error } = await supabase.rpc('get_pending_follow_ups', {
      p_limit: CONFIG.maxFollowUpsPerExecution
    });
    
    if (error) {
      log('error', 'Erro ao buscar follow-ups pendentes', { error: error.message });
      return [];
    }
    
    log('success', `${followUps?.length || 0} follow-ups prontos para execução`);
    return followUps || [];
    
  } catch (error) {
    log('error', 'Erro ao buscar follow-ups', { error: error.message });
    return [];
  }
}

// ===============================================
// CORE: CONTEXTO DA CONVERSA
// ===============================================

/**
 * Busca contexto detalhado da conversa
 */
async function getConversationContext(conversationId) {
  try {
    log('debug', 'Buscando contexto da conversa', { conversationId });
    
    // Buscar dados da conversa
    const { data: conversation, error: convError } = await supabase
      .from('conversations')
      .select(`
        *,
        contact:contacts(*),
        integration:communication_integrations(*)
      `)
      .eq('id', conversationId)
      .single();
      
    if (convError) {
      log('error', 'Erro ao buscar conversa', { error: convError.message, conversationId });
      return null;
    }
    
    // Buscar últimas mensagens (últimas 10)
    const { data: messages, error: msgError } = await supabase
      .from('messages')
      .select('*')
      .eq('conversation_id', conversationId)
      .order('sent_at', { ascending: false })
      .limit(10);
      
    if (msgError) {
      log('warning', 'Erro ao buscar mensagens', { error: msgError.message });
    }
    
    const context = {
      conversation,
      contact: conversation.contact,
      integration: conversation.integration,
      recentMessages: (messages || []).reverse(), // Ordem cronológica
      lastMessage: messages?.[0] || null,
      messageCount: messages?.length || 0,
      hasContactMessages: messages?.some(m => !m.sent_by_ai) || false
    };
    
    log('debug', 'Contexto carregado', {
      conversationId,
      contactName: context.contact?.first_name,
      messageCount: context.messageCount,
      hasContactMessages: context.hasContactMessages,
      lastMessageTime: context.lastMessage?.sent_at
    });
    
    return context;
    
  } catch (error) {
    log('error', 'Erro ao buscar contexto', { error: error.message, conversationId });
    return null;
  }
}

// ===============================================
// CORE: GERAÇÃO DE MENSAGEM COM IA
// ===============================================

/**
 * Busca configurações da OpenAI da empresa
 */
async function getCompanyOpenAIConfig(companyId) {
  try {
    const { data: settings, error } = await supabase
      .from('company_settings')
      .select('api_integrations')
      .eq('company_id', companyId)
      .single();
      
    if (error || !settings?.api_integrations) {
      return null;
    }
    
    const apiConfig = typeof settings.api_integrations === 'string'
      ? JSON.parse(settings.api_integrations)
      : settings.api_integrations;
      
    return apiConfig?.openai?.enabled && apiConfig?.openai?.api_key 
      ? apiConfig.openai 
      : null;
      
  } catch (error) {
    log('error', 'Erro ao buscar config OpenAI', { error: error.message, companyId });
    return null;
  }
}

// ===============================================
// ✅ NOVO: SISTEMA DE CRÉDITOS
// ===============================================

/**
 * Verifica se empresa tem créditos suficientes
 */
async function checkCreditsBalance(companyId, estimatedTokens = CONFIG.credits.estimatedTokensPerFollowUp) {
  try {
    log('debug', 'Verificando saldo de créditos', { companyId, estimatedTokens });
    
    const { data, error } = await supabase
      .from('company_credits')
      .select('balance')
      .eq('company_id', companyId)
      .single();
    
    if (error) {
      log('error', 'Erro ao verificar créditos', { error: error.message, companyId });
      return { hasEnough: false, currentBalance: 0, required: estimatedTokens };
    }
    
    const currentBalance = data?.balance || 0;
    const hasEnough = currentBalance >= estimatedTokens;
    
    log('debug', 'Saldo verificado', { 
      companyId, 
      currentBalance, 
      required: estimatedTokens, 
      hasEnough 
    });
    
    return {
      hasEnough,
      currentBalance,
      required: estimatedTokens
    };
    
  } catch (error) {
    log('error', 'Erro ao verificar créditos', { error: error.message, companyId });
    return { hasEnough: false, currentBalance: 0, required: estimatedTokens };
  }
}

/**
 * Processa consumo de créditos da OpenAI
 */
async function processOpenAICreditsUsage(companyId, totalTokens, conversationId, agentId, description) {
  try {
    log('debug', 'Processando consumo de créditos OpenAI', { 
      companyId, 
      totalTokens, 
      conversationId 
    });
    
    const { data, error } = await supabase.rpc('consume_credits', {
      p_company_id: companyId,
      credits_to_consume: totalTokens, // 1:1 ratio
      service_type: 'openai_followup',
      feature: 'Follow-up Automático',
      description: description,
      user_id: null, // Sistema automático
      tokens_used: totalTokens,
      model_used: 'gpt-4o-mini',
      request_id: conversationId
    });
    
    if (error) {
      log('error', 'Erro ao consumir créditos', { error: error.message, companyId });
      return false;
    }
    
    log('success', 'Créditos consumidos com sucesso', { 
      companyId, 
      tokensUsed: totalTokens,
      creditsConsumed: totalTokens 
    });
    
    return data === true;
    
  } catch (error) {
    log('error', 'Erro ao processar créditos', { error: error.message, companyId });
    return false;
  }
}

/**
 * Estima tokens baseado no texto
 */
function estimateTokensFromText(text) {
  // Aproximação: 4 caracteres = 1 token
  const charCount = text.length;
  const estimatedTokens = Math.ceil(charCount * 0.25);
  
  // Margem de segurança de 20%
  return Math.ceil(estimatedTokens * 1.2);
}

// ===============================================
// CORE: GERAÇÃO DE MENSAGEM COM IA (ATUALIZADA)
// ===============================================

/**
 * Gera mensagem personalizada usando OpenAI com controle de créditos
 */
async function generatePersonalizedMessage(template, context, agent, openaiConfig, companyId) {
  try {
    log('debug', 'Gerando mensagem personalizada com IA');
    
    const contactName = context.contact?.first_name || 'usuário';
    const lastMessages = context.recentMessages
      .slice(-5) // Últimas 5 mensagens
      .map(m => `${m.sent_by_ai ? 'Agente' : contactName}: ${m.content}`)
      .join('\n');
    
    const prompt = `
Você é um assistente de follow-up inteligente. Sua tarefa é reescrever uma mensagem template para reativar uma conversa, baseado no contexto específico da conversa.

AGENTE: ${agent.name}
TOM: ${agent.tone || 'profissional'}
IDIOMA: ${agent.language || 'pt-BR'}

TEMPLATE ORIGINAL:
${template}

CONTEXTO DA CONVERSA:
- Nome do contato: ${contactName}
- Última mensagem enviada: ${formatDuration(Date.now() - new Date(context.lastMessage?.sent_at || Date.now()).getTime())} atrás
- Total de mensagens: ${context.messageCount}
- Conversa prévia (últimas mensagens):
${lastMessages}

INSTRUÇÕES:
1. Reescreva o template para ser mais específico e contextual
2. Mencione algo específico da conversa anterior se relevante
3. Mantenha o tom ${agent.tone || 'profissional'} e ${agent.language || 'português brasileiro'}
4. Seja natural, não robótico
5. Máximo 200 caracteres
6. Não use emojis excessivos

Retorne APENAS a mensagem reescrita, sem explicações.`;

    // ✅ NOVO: Estimar tokens antes da chamada
    const estimatedTokens = estimateTokensFromText(prompt) + 100; // +100 para resposta
    
    // ✅ NOVO: Verificar créditos suficientes
    const creditsCheck = await checkCreditsBalance(companyId, estimatedTokens);
    if (!creditsCheck.hasEnough) {
      log('warning', 'Créditos insuficientes para gerar mensagem IA', {
        companyId,
        currentBalance: creditsCheck.currentBalance,
        required: creditsCheck.required
      });
      
      // Fallback: usar template simples
      return template.replace('{nome}', contactName);
    }

    log('debug', 'Fazendo chamada para OpenAI com controle de créditos', {
      estimatedTokens,
      currentBalance: creditsCheck.currentBalance
    });

    const response = await axios.post('https://api.openai.com/v1/chat/completions', {
      model: openaiConfig.model || 'gpt-4o-mini',
      messages: [
        { role: 'system', content: 'Você é um especialista em follow-up de vendas. Seja direto e eficaz.' },
        { role: 'user', content: prompt }
      ],
      max_tokens: 150,
      temperature: 0.7
    }, {
      headers: {
        'Authorization': `Bearer ${openaiConfig.api_key}`,
        'Content-Type': 'application/json'
      },
      timeout: CONFIG.defaultResponseTimeoutMs
    });
    
    const generatedMessage = response.data.choices[0]?.message?.content?.trim();
    
    if (!generatedMessage) {
      log('warning', 'IA não gerou resposta, usando template original');
      return template.replace('{nome}', contactName);
    }
    
    // ✅ NOVO: Processar consumo de créditos
    const actualTokensUsed = response.data.usage?.total_tokens || estimatedTokens;
    const creditSuccess = await processOpenAICreditsUsage(
      companyId,
      actualTokensUsed,
      context.conversation.id,
      agent.id,
      `Follow-up "${agent.name}" - ${actualTokensUsed} tokens`
    );
    
    if (!creditSuccess) {
      log('warning', 'Falha ao registrar consumo de créditos (mensagem já gerada)');
    }
    
    log('success', 'Mensagem gerada com sucesso', { 
      originalLength: template.length,
      generatedLength: generatedMessage.length,
      tokensUsed: actualTokensUsed,
      creditsProcessed: creditSuccess
    });
    
    return generatedMessage;
    
  } catch (error) {
    log('error', 'Erro ao gerar mensagem com IA', { error: error.message });
    
    // Fallback para template original
    const fallbackMessage = template.replace('{nome}', context.contact?.first_name || 'usuário');
    log('info', 'Usando template fallback', { fallbackMessage });
    return fallbackMessage;
  }
}

// ===============================================
// CORE: ENVIO VIA WHATSAPP
// ===============================================

/**
 * Envia mensagem via WhatsApp
 */
async function sendWhatsAppMessage(instanceName, recipientNumber, message, apiKey, serverUrl) {
  try {
    log('debug', 'Enviando mensagem WhatsApp', { 
      instanceName, 
      recipientNumber: recipientNumber.substring(0, 8) + '...',
      messageLength: message.length 
    });
    
    const response = await axios.post(`${serverUrl}/message/sendText/${instanceName}`, {
      number: recipientNumber,
      text: message
    }, {
      headers: {
        'apikey': apiKey,
        'Content-Type': 'application/json'
      },
      timeout: CONFIG.defaultResponseTimeoutMs
    });
    
    if (response.data.error) {
      throw new Error(response.data.message || 'Erro no envio WhatsApp');
    }
    
    log('success', 'Mensagem WhatsApp enviada', {
      instanceName,
      recipientNumber: recipientNumber.substring(0, 8) + '...',
      messageId: response.data.key?.id
    });
    
    return { success: true, messageId: response.data.key?.id };
    
  } catch (error) {
    log('error', 'Erro ao enviar WhatsApp', { 
      error: error.message,
      instanceName,
      recipientNumber: recipientNumber.substring(0, 8) + '...'
    });
    return { success: false, error: error.message };
  }
}

// ===============================================
// CORE: PROCESSAR FOLLOW-UP
// ===============================================

/**
 * Processa um único follow-up
 */
async function processFollowUp(followUp) {
  const startTime = Date.now();
  let executionLog = {
    follow_up_queue_id: followUp.id,
    agent_id: followUp.agent_id,
    conversation_id: followUp.conversation_id,
    contact_id: followUp.contact_id,
    company_id: followUp.company_id,
    rule_name: followUp.rule_name,
    success: false,
    error_message: null,
    response_time_ms: 0,
    message_sent: '',
    conversation_reactivated: false
  };
  
  try {
    log('info', `Processando follow-up: ${followUp.rule_name}`, { followUpId: followUp.id });
    
    // ✅ NOVO: Verificar créditos mínimos da empresa
    const creditsCheck = await checkCreditsBalance(followUp.company_id, CONFIG.credits.minimumBalanceThreshold);
    if (!creditsCheck.hasEnough) {
      throw new Error(`Créditos insuficientes (${creditsCheck.currentBalance}/${CONFIG.credits.minimumBalanceThreshold})`);
    }
    
    // 1. Buscar contexto da conversa
    const context = await getConversationContext(followUp.conversation_id);
    if (!context) {
      throw new Error('Não foi possível carregar contexto da conversa');
    }
    
    // 2. Buscar dados do agente
    const { data: agent, error: agentError } = await supabase
      .from('ai_agents')
      .select('*')
      .eq('id', followUp.agent_id)
      .single();
      
    if (agentError || !agent) {
      throw new Error(`Agente não encontrado: ${agentError?.message}`);
    }
    
    // 3. Verificar se deve executar (horário comercial se configurado)
    const followUpRules = agent.follow_up_rules || [];
    const rule = followUpRules.find(r => r.id === followUp.rule_id);
    
    if (rule?.conditions?.exclude_business_hours && !isBusinessHours()) {
      log('info', 'Follow-up adiado - fora do horário comercial', { followUpId: followUp.id });
      
      // Reagendar para próximo horário comercial
      const nextBusinessHour = new Date();
      nextBusinessHour.setHours(CONFIG.businessHours.start, 0, 0, 0);
      if (nextBusinessHour <= new Date()) {
        nextBusinessHour.setDate(nextBusinessHour.getDate() + 1);
      }
      
      await supabase
        .from('follow_up_queue')
        .update({ scheduled_at: nextBusinessHour.toISOString() })
        .eq('id', followUp.id);
        
      return { success: true, deferred: true };
    }
    
    // 4. Buscar configuração OpenAI
    const openaiConfig = await getCompanyOpenAIConfig(followUp.company_id);
    
    // 5. Gerar mensagem personalizada (✅ COM CONTROLE DE CRÉDITOS)
    let finalMessage = followUp.message_template;
    
    if (openaiConfig) {
      finalMessage = await generatePersonalizedMessage(
        followUp.message_template,
        context,
        agent,
        openaiConfig,
        followUp.company_id // ✅ NOVO: Passar company_id
      );
    } else {
      // Fallback simples
      finalMessage = finalMessage.replace('{nome}', context.contact?.first_name || 'usuário');
    }
    
    executionLog.message_sent = finalMessage;
    
    // 6. Buscar dados da instância WhatsApp
    const integration = context.integration;
    if (!integration?.server_url || !integration?.api_key) {
      throw new Error('Configuração WhatsApp não encontrada');
    }
    
    // 7. Enviar mensagem
    const sendResult = await sendWhatsAppMessage(
      integration.instance_name,
      context.contact.phone,
      finalMessage,
      integration.api_key,
      integration.server_url
    );
    
    if (!sendResult.success) {
      throw new Error(sendResult.error);
    }
    
    // 8. Marcar como enviado
    await supabase
      .from('follow_up_queue')
      .update({ 
        status: 'sent',
        attempts: followUp.attempts + 1,
        executed_at: new Date().toISOString(),
        ai_generated_message: finalMessage
      })
      .eq('id', followUp.id);
    
    // 9. Registrar mensagem no sistema
    await supabase
      .from('messages')
      .insert({
        conversation_id: followUp.conversation_id,
        contact_id: followUp.contact_id,
        direction: 'outbound',
        message_type: 'text',
        content: finalMessage,
        sent_at: new Date().toISOString(),
        sent_by_ai: true,
        ai_agent_id: followUp.agent_id,
        metadata: {
          follow_up_id: followUp.id,
          rule_name: followUp.rule_name,
          is_follow_up: true
        }
      });
    
    executionLog.success = true;
    executionLog.response_time_ms = Date.now() - startTime;
    
    log('success', `Follow-up enviado com sucesso`, {
      followUpId: followUp.id,
      ruleName: followUp.rule_name,
      contactName: context.contact?.first_name,
      responseTime: formatDuration(executionLog.response_time_ms)
    });
    
    return { success: true, messageId: sendResult.messageId };
    
  } catch (error) {
    executionLog.error_message = error.message;
    executionLog.response_time_ms = Date.now() - startTime;
    
    // Atualizar tentativas
    const newAttempts = followUp.attempts + 1;
    const status = newAttempts >= followUp.max_attempts ? 'failed' : 'pending';
    
    await supabase
      .from('follow_up_queue')
      .update({ 
        attempts: newAttempts,
        status: status,
        execution_error: error.message
      })
      .eq('id', followUp.id);
    
    log('error', `Erro ao processar follow-up`, {
      followUpId: followUp.id,
      error: error.message,
      attempts: newAttempts,
      maxAttempts: followUp.max_attempts,
      finalStatus: status
    });
    
    return { success: false, error: error.message };
    
  } finally {
    // Registrar log de execução
    try {
      await supabase.from('follow_up_logs').insert(executionLog);
    } catch (logError) {
      log('warning', 'Erro ao registrar log de execução', { error: logError.message });
    }
  }
}

// ===============================================
// ✅ NOVO: SINCRONIZAÇÃO DE FOLLOW-UPS ÓRFÃOS
// ===============================================

/**
 * Busca e cria follow-ups órfãos baseado nas regras dos agentes
 */
async function findAndCreateOrphanedFollowUps() {
  try {
    log('info', '🔍 Verificando conversas órfãs...');
    
    // 1. Buscar conversas com agentes ativos que podem precisar de follow-up
    const { data: conversations, error: convError } = await supabase
      .from('conversations')
      .select(`
        id,
        ai_agent_id,
        contact_id,
        metadata,
        ai_agents!inner(
          id,
          company_id,
          name,
          status,
          follow_up_rules
        ),
        contacts!inner(
          id,
          first_name,
          phone
        )
      `)
      .eq('ai_agents.status', 'active')
      .not('ai_agents.follow_up_rules', 'is', null)
      .gte('updated_at', new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString()) // Últimos 7 dias
      .limit(100); // Limitar para não sobrecarregar

    if (convError) {
      log('error', 'Erro ao buscar conversas para sincronização', { error: convError.message });
      return [];
    }

    if (!conversations || conversations.length === 0) {
      log('info', 'Nenhuma conversa com agente ativo encontrada');
      return [];
    }

    log('info', `Verificando ${conversations.length} conversas com agentes ativos...`);

    const orphanedFollowUps = [];

    // 2. Para cada conversa, verificar se precisa de follow-ups
    for (const conversation of conversations) {
      try {
        // Verificar se follow-up está pausado manualmente
        if (conversation.metadata?.follow_up_paused === true) {
          continue;
        }

        const agent = conversation.ai_agents;
        const followUpRules = agent.follow_up_rules || [];

        if (followUpRules.length === 0) {
          continue;
        }

        // 3. Buscar última mensagem da conversa
        const { data: lastMessages, error: msgError } = await supabase
          .from('messages')
          .select('sent_at, sent_by_ai, content')
          .eq('conversation_id', conversation.id)
          .order('sent_at', { ascending: false })
          .limit(2);

        if (msgError) {
          log('warning', 'Erro ao buscar mensagens da conversa', { 
            conversationId: conversation.id, 
            error: msgError.message 
          });
          continue;
        }

        if (!lastMessages || lastMessages.length === 0) {
          continue;
        }

        const lastMessage = lastMessages[0];
        const lastMessageTime = new Date(lastMessage.sent_at);
        const now = new Date();

        // 4. Para cada regra ativa do agente
        for (const rule of followUpRules) {
          if (!rule.is_active) {
            continue;
          }

          const delayMinutes = rule.delay_minutes || 15;
          const ruleId = rule.id || rule.name?.replace(/\s+/g, '_').toLowerCase();
          
          if (!ruleId) {
            log('warning', 'Regra sem ID válido encontrada', { 
              agentId: agent.id, 
              ruleName: rule.name 
            });
            continue;
          }

          // 5. Verificar se tempo da regra já passou
          const minutesSinceLastMessage = (now.getTime() - lastMessageTime.getTime()) / (1000 * 60);
          
          if (minutesSinceLastMessage < delayMinutes) {
            continue; // Ainda não é hora desta regra
          }

          // 6. Verificar se já existe follow-up para esta regra/conversa
          const { data: existingFollowUp, error: existingError } = await supabase
            .from('follow_up_queue')
            .select('id, status')
            .eq('conversation_id', conversation.id)
            .eq('rule_id', ruleId)
            .single();

          if (existingError && existingError.code !== 'PGRST116') { // PGRST116 = not found
            log('warning', 'Erro ao verificar follow-up existente', { 
              conversationId: conversation.id,
              ruleId: ruleId,
              error: existingError.message 
            });
            continue;
          }

          if (existingFollowUp) {
            continue; // Já existe follow-up para esta regra
          }

          // 7. Criar follow-up órfão retroativo
          const scheduledAt = new Date(lastMessageTime.getTime() + (delayMinutes * 60 * 1000));
          
          log('info', `Criando follow-up órfão retroativo`, {
            conversationId: conversation.id,
            contactName: conversation.contacts.first_name,
            agentName: agent.name,
            ruleName: rule.name,
            delayMinutes: delayMinutes,
            minutesLate: Math.round(minutesSinceLastMessage - delayMinutes),
            scheduledAt: scheduledAt.toISOString()
          });

          const { data: newFollowUp, error: insertError } = await supabase
            .from('follow_up_queue')
            .insert({
              agent_id: agent.id,
              conversation_id: conversation.id,
              contact_id: conversation.contact_id,
              company_id: agent.company_id,
              rule_id: ruleId,
              rule_name: rule.name || `Regra ${ruleId}`,
              scheduled_at: scheduledAt.toISOString(),
              last_message_at: lastMessage.sent_at,
              message_template: rule.message_template || 'Olá {nome}! Como posso continuar ajudando você?',
              max_attempts: rule.max_attempts || 1,
              status: 'pending',
              attempts: 0,
              metadata: {
                created_by: 'orphan_sync',
                minutes_late: Math.round(minutesSinceLastMessage - delayMinutes),
                sync_timestamp: now.toISOString(),
                delay_minutes: delayMinutes
              }
            })
            .select('id, rule_name, scheduled_at')
            .single();

          if (insertError) {
            log('error', 'Erro ao criar follow-up órfão', {
              conversationId: conversation.id,
              ruleId: ruleId,
              error: insertError.message
            });
            continue;
          }

          orphanedFollowUps.push(newFollowUp);
          
          log('success', `Follow-up órfão criado com sucesso`, {
            followUpId: newFollowUp.id,
            conversationId: conversation.id,
            ruleName: newFollowUp.rule_name,
            contactName: conversation.contacts.first_name
          });
        }

        // Pequena pausa entre conversas para não sobrecarregar
        await new Promise(resolve => setTimeout(resolve, 100));

      } catch (convError) {
        log('error', 'Erro ao processar conversa para sincronização', {
          conversationId: conversation.id,
          error: convError.message
        });
        continue;
      }
    }

    if (orphanedFollowUps.length > 0) {
      log('success', `✅ Sincronização concluída: ${orphanedFollowUps.length} follow-ups órfãos criados`);
    } else {
      log('info', 'Nenhum follow-up órfão encontrado');
    }

    return orphanedFollowUps;

  } catch (error) {
    log('error', 'Erro na sincronização de follow-ups órfãos', { error: error.message });
    return [];
  }
}

// ===============================================
// CORE: EXECUÇÃO PRINCIPAL (ATUALIZADA)
// ===============================================

/**
 * Execução principal do processamento de follow-ups
 */
async function executeFollowUps() {
  const executionStart = Date.now();
  stats.totalExecutions++;
  
  log('info', '🔄 === INICIANDO EXECUÇÃO DE FOLLOW-UPS ===', {
    execution: stats.totalExecutions,
    timestamp: new Date().toISOString()
  });
  
  try {
    // 1. Buscar follow-ups pendentes existentes
    const pendingFollowUps = await getPendingFollowUps();
    
    // ✅ 2. NOVO: Buscar e criar follow-ups órfãos
    const orphanedFollowUps = await findAndCreateOrphanedFollowUps();
    
    // 3. Combinar ambos os tipos
    const allFollowUps = [...pendingFollowUps, ...orphanedFollowUps];
    
    if (allFollowUps.length === 0) {
      log('info', 'Nenhum follow-up para processar');
      return;
    }
    
    log('info', `Processando ${allFollowUps.length} follow-ups (${pendingFollowUps.length} pendentes + ${orphanedFollowUps.length} órfãos)...`);
    
    // 4. Processar cada follow-up
    const results = [];
    for (const followUp of allFollowUps) {
      const result = await processFollowUp(followUp);
      results.push(result);
      
      // Pausa entre execuções para não sobrecarregar
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    // 5. Calcular estatísticas
    const successful = results.filter(r => r.success && !r.deferred).length;
    const deferred = results.filter(r => r.deferred).length;
    const failed = results.filter(r => !r.success).length;
    
    stats.totalFollowUpsSent += successful;
    stats.totalOrphansCreated += orphanedFollowUps.length; // ✅ NOVO: Contar órfãos criados
    stats.successRate = stats.totalFollowUpsSent / (stats.totalFollowUpsSent + stats.totalErrors) * 100;
    stats.lastExecution = new Date();
    
    if (failed > 0) {
      stats.totalErrors += failed;
    }
    
    const executionTime = Date.now() - executionStart;
    
    log('success', '✅ === EXECUÇÃO CONCLUÍDA ===', {
      totalProcessed: allFollowUps.length,
      pendingProcessed: pendingFollowUps.length,
      orphansCreated: orphanedFollowUps.length,
      successful,
      deferred,
      failed,
      executionTime: formatDuration(executionTime),
      successRate: `${stats.successRate.toFixed(1)}%`
    });
    
  } catch (error) {
    stats.totalErrors++;
    log('error', 'Erro na execução principal', { error: error.message });
  }
}

// ===============================================
// API DE STATUS
// ===============================================

/**
 * Endpoint simples para status do servidor
 */
function startStatusEndpoint() {
  const express = require('express');
  const app = express();
  const PORT = process.env.PORT || 3000;
  
  app.get('/', (req, res) => {
    res.json({
      status: 'running',
      service: 'Zionic Follow-up Server',
      version: '1.2.0', // ✅ ATUALIZADO: Nova versão com sincronização órfã
      uptime: formatDuration(Date.now() - stats.serverStartTime),
      stats: {
        ...stats,
        nextExecution: 'A cada 1 minuto (máxima precisão)'
      },
      features: {
        orphanSync: true,
        creditsControl: true,
        intervalMinutes: CONFIG.executionIntervalMinutes
      },
      timestamp: new Date().toISOString()
    });
  });
  
  app.get('/health', (req, res) => {
    res.json({ 
      status: 'healthy',
      timestamp: new Date().toISOString(),
      lastExecution: stats.lastExecution
    });
  });
  
  app.listen(PORT, () => {
    log('success', `API de status rodando na porta ${PORT}`);
  });
}

// ===============================================
// INICIALIZAÇÃO
// ===============================================

async function initialize() {
  try {
    log('info', 'Testando conexão com Supabase...');
    
    const { data, error } = await supabase.from('ai_agents').select('id').limit(1);
    if (error) {
      throw new Error(`Erro na conexão com Supabase: ${error.message}`);
    }
    
    log('success', 'Conexão com Supabase estabelecida');
    
    // ✅ ATUALIZADO: Configurar cron job para executar a cada 1 minuto (maior precisão)
    cron.schedule('*/1 * * * *', () => {
      executeFollowUps().catch(error => {
        log('error', 'Erro no cron job', { error: error.message });
      });
    });
    
    log('success', 'Cron job configurado (a cada 1 minuto para máxima precisão)');
    
    // Iniciar API de status
    startStatusEndpoint();
    
    // Executar imediatamente uma vez
    await executeFollowUps();
    
    log('success', '🎉 Servidor de follow-up inicializado com sucesso!');
    
  } catch (error) {
    log('error', 'Falha na inicialização', { error: error.message });
    process.exit(1);
  }
}

// Tratar sinais de encerramento
process.on('SIGTERM', () => {
  log('info', 'Servidor recebeu SIGTERM, encerrando...');
  process.exit(0);
});

process.on('SIGINT', () => {
  log('info', 'Servidor recebeu SIGINT, encerrando...');
  process.exit(0);
});

// Inicializar servidor
initialize();

module.exports = {
  executeFollowUps,
  processFollowUp,
  stats
}; 
