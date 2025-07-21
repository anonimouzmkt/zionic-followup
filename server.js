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
 * - Envia via WhatsApp (Evolution API)
 * - Registra logs e métricas
 * ✅ CONTROLE AUTOMÁTICO DE CRÉDITOS
 * ✅ SINCRONIZAÇÃO DE ÓRFÃOS
 * 
 * ENV VARS necessárias:
 * - SUPABASE_URL
 * - SUPABASE_SERVICE_ROLE_KEY  
 * - EVOLUTION_API_URL
 * - EVOLUTION_API_KEY
 * - ZIONIC_OPENAI_KEY (ou OPENAI_API_KEY) - para Zionic Credits
 * 
 * Deploy: Render.com
 * Frequência: A cada 1 minuto
 * 
 * @author Zionic Team
 * @version 1.5.0
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

// ✅ NOVO: Configurar Evolution API (mesmo que conversation.js)
const EVOLUTION_API_URL = process.env.EVOLUTION_API_URL || 'https://evowise.anonimouz.com';
const EVOLUTION_API_KEY = process.env.EVOLUTION_API_KEY || 'GfwncPVPb2ou4i1DMI9IEAVVR3p0fI7W';

// Validar variáveis obrigatórias
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ ERRO: Variáveis de ambiente do Supabase não configuradas');
  console.error('Necessário: SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

console.log('🔧 Evolution API configurada:', {
  url: EVOLUTION_API_URL,
  keyConfigured: !!EVOLUTION_API_KEY
});

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
 * ✅ CORRIGIDO: Verifica se está dentro do horário comercial considerando timezone da empresa
 */
async function isBusinessHours(companyId) {
  try {
    // ✅ Buscar timezone da empresa ou usuário
    const timezone = await getCompanyTimezone(companyId);
    
    // ✅ Obter hora atual no timezone da empresa
  const now = new Date();
    const companyTime = new Intl.DateTimeFormat('en-CA', {
      timeZone: timezone,
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    }).format(now);
    
    const currentHour = parseInt(companyTime.split(':')[0]);
    
    log('debug', 'Verificação de horário comercial', {
      companyId,
      timezone,
      serverTime: now.toISOString(),
      companyTime,
      currentHour,
      businessStart: CONFIG.businessHours.start,
      businessEnd: CONFIG.businessHours.end,
      isWithinHours: currentHour >= CONFIG.businessHours.start && currentHour < CONFIG.businessHours.end
    });
    
    return currentHour >= CONFIG.businessHours.start && currentHour < CONFIG.businessHours.end;
    
  } catch (error) {
    log('error', 'Erro ao verificar horário comercial', { error: error.message, companyId });
    // Fallback: assumir horário comercial em caso de erro
    return true;
  }
}

/**
 * ✅ NOVO: Busca timezone da empresa ou usuário
 */
async function getCompanyTimezone(companyId) {
  try {
    // 1. Tentar buscar timezone da empresa primeiro
    const { data: company, error: companyError } = await supabase
      .from('companies')
      .select('timezone')
      .eq('id', companyId)
      .single();
    
    if (!companyError && company?.timezone) {
      log('debug', 'Timezone encontrado na empresa', { companyId, timezone: company.timezone });
      return company.timezone;
    }
    
    // 2. Se não encontrar na empresa, buscar do usuário admin/owner da empresa
    const { data: user, error: userError } = await supabase
      .from('users')
      .select('timezone')
      .eq('company_id', companyId)
      .eq('is_owner', true)
      .single();
    
    if (!userError && user?.timezone) {
      log('debug', 'Timezone encontrado no usuário owner', { companyId, timezone: user.timezone });
      return user.timezone;
    }
    
    // 3. Fallback para timezone padrão brasileiro
    log('debug', 'Usando timezone padrão (fallback)', { companyId, timezone: 'America/Sao_Paulo' });
    return 'America/Sao_Paulo';
    
  } catch (error) {
    log('error', 'Erro ao buscar timezone da empresa', { error: error.message, companyId });
    return 'America/Sao_Paulo';
  }
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
 * ✅ OTIMIZADO: Busca follow-ups prontos para execução com validações extras
 */
async function getPendingFollowUps() {
  try {
    log('info', 'Buscando follow-ups pendentes...');
    
    const { data: followUps, error } = await supabase.rpc('get_pending_follow_ups_optimized', {
      p_limit: CONFIG.maxFollowUpsPerExecution
    });
    
    if (error) {
      log('error', 'Erro ao buscar follow-ups pendentes', { error: error.message });
      return [];
    }
    
    const totalPending = followUps?.length || 0;
    const overdueCount = followUps?.filter(f => f.minutes_overdue > 0).length || 0;
    
    log('success', `${totalPending} follow-ups prontos para execução`, {
      total: totalPending,
      overdue: overdueCount,
      onTime: totalPending - overdueCount,
      method: 'sql_optimized'
    });
    
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
    
    // Buscar dados da conversa (simplificado - sem integration)
    const { data: conversation, error: convError } = await supabase
      .from('conversations')
      .select(`
        *,
        contact:contacts(*)
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
      lastMessageTime: context.lastMessage?.sent_at,
      companyId: conversation.company_id
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



// ===============================================
// ✅ NOVO: SISTEMA DE CRÉDITOS
// ===============================================

/**
 * Verifica se empresa tem créditos suficientes
 */
async function checkCreditsBalance(companyId, estimatedTokens = CONFIG.credits.estimatedTokensPerFollowUp) {
  try {
    // ✅ VALIDAÇÃO DE ENTRADA
    if (!companyId || companyId === 'undefined' || companyId === null) {
      log('error', 'CompanyId inválido para verificação de créditos', { 
        companyId, 
        type: typeof companyId,
        estimatedTokens 
      });
      return { hasEnough: false, currentBalance: 0, required: estimatedTokens, error: 'CompanyId inválido' };
    }
    
    log('debug', 'Verificando saldo de créditos', { 
      companyId: companyId.toString(), 
      estimatedTokens,
      companyIdType: typeof companyId
    });
    
    const { data, error } = await supabase
      .from('company_credits')
      .select('balance')
      .eq('company_id', companyId)
      .single();
    
    if (error) {
      log('error', 'Erro ao verificar créditos', { 
        error: error.message, 
        companyId: companyId.toString(),
        errorCode: error.code
      });
      return { hasEnough: false, currentBalance: 0, required: estimatedTokens, error: error.message };
    }
    
    const currentBalance = data?.balance || 0;
    const hasEnough = currentBalance >= estimatedTokens;
    
    log('debug', 'Saldo verificado', { 
      companyId: companyId.toString(), 
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
    log('error', 'Erro ao verificar créditos', { 
      error: error.message, 
      companyId: companyId ? companyId.toString() : 'null/undefined',
      stack: error.stack
    });
    return { hasEnough: false, currentBalance: 0, required: estimatedTokens, error: error.message };
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
    
    // ✅ NOVO: Verificar saldo antes do consumo
    const { data: creditsBefore, error: balanceError } = await supabase
      .from('company_credits')
      .select('balance')
      .eq('company_id', companyId)
      .single();
    
    if (balanceError) {
      log('error', 'Erro ao verificar saldo antes do consumo', { error: balanceError.message, companyId });
    } else {
      log('debug', 'Saldo ANTES do consumo', { 
        companyId, 
        saldoAntes: creditsBefore?.balance || 0 
      });
    }
    
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
    
    // ✅ NOVO: Log detalhado da resposta da função
    log('debug', 'Resposta da função consume_credits', { 
      data, 
      error: error?.message,
      functionResult: data
    });
    
    if (error) {
      log('error', 'Erro ao consumir créditos', { 
        error: error.message, 
        companyId,
        errorCode: error.code,
        errorDetails: error.details,
        errorHint: error.hint
      });
      return false;
    }
    
    // ✅ NOVO: Verificar saldo depois do consumo
    const { data: creditsAfter, error: balanceAfterError } = await supabase
      .from('company_credits')
      .select('balance')
      .eq('company_id', companyId)
      .single();
    
    if (balanceAfterError) {
      log('error', 'Erro ao verificar saldo depois do consumo', { error: balanceAfterError.message, companyId });
    } else {
      const saldoAntes = creditsBefore?.balance || 0;
      const saldoDepois = creditsAfter?.balance || 0;
      const diferencaEsperada = totalTokens;
      const diferencaReal = saldoAntes - saldoDepois;
      
      log('success', 'Comparação de saldos', { 
        companyId, 
        tokensUsed: totalTokens,
        saldoAntes,
        saldoDepois,
        diferencaEsperada,
        diferencaReal,
        funcionouCorreto: diferencaReal === diferencaEsperada
      });
      
      if (diferencaReal !== diferencaEsperada) {
        log('error', '❌ PROBLEMA: Saldo não foi atualizado corretamente!', {
          companyId,
          esperado: diferencaEsperada,
          real: diferencaReal,
          funcionResult: data
        });
      }
    }
    
    log('success', 'Créditos consumidos com sucesso', { 
      companyId, 
      tokensUsed: totalTokens,
      creditsConsumed: totalTokens,
      functionReturnedTrue: data === true
    });
    
    return data === true;
    
  } catch (error) {
    log('error', 'Erro ao processar créditos', { 
      error: error.message, 
      companyId,
      stack: error.stack
    });
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
 * ✅ CORRIGIDO: Gera mensagem usando Zionic Credits com THREADS PERSISTENTES
 */
async function generatePersonalizedMessageWithZionicCredits(template, context, agent, companyId) {
  try {
    log('info', 'Gerando mensagem com Zionic Credits usando threads persistentes', { companyId });
    
    // ✅ USAR CHAVE ZIONIC OPENAI (do sistema)
    const ZIONIC_OPENAI_KEY = process.env.ZIONIC_OPENAI_KEY || process.env.OPENAI_API_KEY;
    
    if (!ZIONIC_OPENAI_KEY) {
      log('error', 'Chave OpenAI do sistema Zionic não configurada');
      return template.replace('{nome}', context.contact?.first_name || 'usuário');
    }

    // Verificar créditos Zionic suficientes (estimativa conservadora)
    const creditsCheck = await checkCreditsBalance(companyId, 300); // Estimativa para threads + assistant
    if (!creditsCheck.hasEnough) {
      log('warning', 'Créditos Zionic insuficientes para threads + assistant', {
        companyId,
        currentBalance: creditsCheck.currentBalance,
        required: 300
      });
      
      await notifyZionicCreditsInsufficient(companyId, creditsCheck.currentBalance, 300);
      return template.replace('{nome}', context.contact?.first_name || 'usuário');
    }

    // ✅ USAR THREADS PERSISTENTES (igual ao webhook principal)
    let assistantMessage;
    
    if (agent.openai_assistant_id) {
      // Modo assistant (preferido)
      log('debug', 'Usando OpenAI Assistant com threads persistentes', { 
        assistantId: agent.openai_assistant_id,
        conversationId: context.conversation.id
      });
      
      assistantMessage = await generateWithAssistantAndThread(
        ZIONIC_OPENAI_KEY,
        agent,
        template,
        context,
        companyId
      );
    } else {
      // Fallback: usar thread + modelo direto
      log('debug', 'Usando thread com modelo direto (fallback)', { 
        model: agent.openai_model || 'gpt-4o-mini',
        conversationId: context.conversation.id
      });
      
      assistantMessage = await generateWithThreadOnly(
        ZIONIC_OPENAI_KEY,
        agent,
        template,
        context,
        companyId
      );
    }
    
    if (!assistantMessage) {
      log('warning', 'IA Zionic com threads não gerou resposta, usando template original');
      return template.replace('{nome}', context.contact?.first_name || 'usuário');
    }
    
    log('success', 'Mensagem gerada com Zionic Credits (threads persistentes)', { 
      originalLength: template.length,
      generatedLength: assistantMessage.length,
      mode: 'zionic_credits_threads',
      hasAssistant: !!agent.openai_assistant_id
    });
    
    return assistantMessage;
    
  } catch (error) {
    log('error', 'Erro ao gerar mensagem com Zionic Credits + threads', { error: error.message, companyId });
    
    await notifyOpenAIError(companyId, { 
      status: error.response?.status,
      message: error.message 
    }, 'master_key');
    
    // Fallback para template original
    const fallbackMessage = template.replace('{nome}', context.contact?.first_name || 'usuário');
    log('info', 'Usando template fallback após erro Zionic Credits + threads', { fallbackMessage });
    return fallbackMessage;
  }
}

// ===============================================
// THREADS PERSISTENTES OPENAI (IGUAL AO WEBHOOK PRINCIPAL)
// ===============================================

/**
 * ✅ NOVO: Gera mensagem usando Assistant + Thread (modo preferido)
 */
async function generateWithAssistantAndThread(apiKey, agent, template, context, companyId) {
  try {
    log('debug', 'Iniciando geração com Assistant + Thread');
    
    // 1. Obter ou criar thread OpenAI
    const threadId = await getOrCreateOpenAIThread(apiKey, context.conversation.id, agent, {
      contactName: context.contact?.first_name || 'Cliente',
      contactPhone: context.contact?.phone || '',
      contactData: {}
    });
    
    if (!threadId) {
      throw new Error('Falha ao criar thread OpenAI');
    }
    
    // 2. Adicionar mensagem específica para follow-up na thread
    const followUpPrompt = buildFollowUpPrompt(template, context, agent);
    await addMessageToOpenAIThread(apiKey, threadId, followUpPrompt, 'user');
    
    // 3. Executar run do assistant
    const runResult = await executeOpenAIRun(apiKey, threadId, agent, context.conversation.id, context.contact?.phone || '');
    
    if (!runResult.success) {
      throw new Error(`Run falhou: ${runResult.error}`);
    }
    
    // 4. Obter resposta da thread
    const assistantMessage = await getLatestAssistantMessage(apiKey, threadId);
    
    if (!assistantMessage) {
      throw new Error('Nenhuma resposta do assistant');
    }
    
    // 5. Processar consumo de créditos (se tiver usage)
    if (runResult.usage?.total_tokens) {
      await processOpenAICreditsUsage(
      companyId,
        runResult.usage.total_tokens,
      context.conversation.id,
      agent.id,
        `Follow-up Assistant "${agent.name}" - ${runResult.usage.total_tokens} tokens`
      );
    }
    
    return assistantMessage.trim();
    
  } catch (error) {
    log('error', 'Erro na geração com Assistant + Thread', { error: error.message });
    throw error;
  }
}

/**
 * ✅ NOVO: Gera mensagem usando Thread + modelo direto (fallback)
 */
async function generateWithThreadOnly(apiKey, agent, template, context, companyId) {
  try {
    log('debug', 'Iniciando geração com Thread + modelo direto');
    
    // 1. Obter ou criar thread OpenAI
    const threadId = await getOrCreateOpenAIThread(apiKey, context.conversation.id, agent, {
      contactName: context.contact?.first_name || 'Cliente',
      contactPhone: context.contact?.phone || '',
      contactData: {}
    });
    
    if (!threadId) {
      throw new Error('Falha ao criar thread OpenAI');
    }
    
    // 2. Adicionar mensagem específica para follow-up na thread
    const followUpPrompt = buildFollowUpPrompt(template, context, agent);
    await addMessageToOpenAIThread(apiKey, threadId, followUpPrompt, 'user');
    
    // 3. Executar run com modelo direto (sem assistant)
    const runResult = await executeOpenAIRunWithModel(apiKey, threadId, agent, context.conversation.id);
    
    if (!runResult.success) {
      throw new Error(`Run com modelo falhou: ${runResult.error}`);
    }
    
    // 4. Obter resposta da thread
    const assistantMessage = await getLatestAssistantMessage(apiKey, threadId);
    
    if (!assistantMessage) {
      throw new Error('Nenhuma resposta do modelo na thread');
    }
    
    // 5. Processar consumo de créditos (se tiver usage)
    if (runResult.usage?.total_tokens) {
      await processOpenAICreditsUsage(
        companyId,
        runResult.usage.total_tokens,
        context.conversation.id,
        agent.id,
        `Follow-up Thread "${agent.name}" - ${runResult.usage.total_tokens} tokens`
      );
    }
    
    return assistantMessage.trim();
    
  } catch (error) {
    log('error', 'Erro na geração com Thread + modelo', { error: error.message });
    throw error;
  }
}

/**
 * ✅ NOVO: Constrói prompt específico para follow-up
 */
function buildFollowUpPrompt(template, context, agent) {
    const contactName = context.contact?.first_name || 'usuário';
    const lastMessages = context.recentMessages
    .slice(-3) // Últimas 3 mensagens (thread já tem o histórico)
      .map(m => `${m.sent_by_ai ? 'Agente' : contactName}: ${m.content}`)
      .join('\n');
    
  return `FOLLOW-UP AUTOMÁTICO: Você precisa reativar esta conversa que parou de responder.

TEMPLATE ORIGINAL: "${template}"

CONTEXTO ATUAL:
- Nome: ${contactName}
- Última interação: ${formatDuration(Date.now() - new Date(context.lastMessage?.sent_at || Date.now()).getTime())} atrás
- Últimas mensagens:
${lastMessages}

INSTRUÇÃO: Reescreva o template de forma mais personalizada baseada no contexto da conversa anterior. Seja natural e específico. Máximo 150 caracteres.

Responda APENAS com a mensagem reescrita, sem explicações.`;
}

/**
 * ✅ NOVO: Obter ou criar thread OpenAI (baseado no webhook)
 */
async function getOrCreateOpenAIThread(apiKey, conversationId, agent, context) {
  try {
    // 1. Verificar se conversa já tem thread
    const { data: conversation, error } = await supabase
      .from('conversations')
      .select('openai_thread_id')
      .eq('id', conversationId)
      .single();
      
    if (error) {
      log('error', 'Erro ao buscar thread existente', { error: error.message });
      return null;
    }
    
    if (conversation?.openai_thread_id) {
      log('debug', 'Reutilizando thread existente', { threadId: conversation.openai_thread_id });
      return conversation.openai_thread_id;
    }
    
    // 2. Criar nova thread
    log('debug', 'Criando nova thread OpenAI');
    const response = await axios.post('https://api.openai.com/v1/threads', {}, {
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'OpenAI-Beta': 'assistants=v2'
      },
      timeout: CONFIG.defaultResponseTimeoutMs
    });
    
    const threadId = response.data.id;
    
    // 3. Salvar thread na conversa
    await supabase
      .from('conversations')
      .update({ 
        openai_thread_id: threadId,
        updated_at: new Date().toISOString()
      })
      .eq('id', conversationId);
    
    // 4. Adicionar mensagem de sistema inicial
    await addSystemMessageToThread(apiKey, threadId, agent, context);
    
    log('success', 'Thread OpenAI criada e salva', { threadId, conversationId });
    return threadId;
    
  } catch (error) {
    log('error', 'Erro ao criar thread OpenAI', { error: error.message });
    return null;
  }
}

/**
 * ✅ NOVO: Adicionar mensagem de sistema à thread
 */
async function addSystemMessageToThread(apiKey, threadId, agent, context) {
  try {
    const systemPrompt = buildSystemPromptForFollowUp(agent, context);
    
    await axios.post(`https://api.openai.com/v1/threads/${threadId}/messages`, {
      role: 'system',
      content: systemPrompt
    }, {
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'OpenAI-Beta': 'assistants=v2'
      },
      timeout: CONFIG.defaultResponseTimeoutMs
    });
    
    log('debug', 'Mensagem de sistema adicionada à thread');
    
  } catch (error) {
    log('error', 'Erro ao adicionar mensagem de sistema', { error: error.message });
  }
}

/**
 * ✅ NOVO: Construir prompt de sistema para follow-up
 */
function buildSystemPromptForFollowUp(agent, context) {
  const now = new Date();
  const currentDateTime = new Intl.DateTimeFormat('pt-BR', {
    timeZone: 'America/Sao_Paulo',
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  }).format(now);

  return `Você é ${agent.name}, um assistente especializado em follow-up de vendas.

CONTEXTO: Hoje é ${currentDateTime}

SEU PAPEL: Reativar conversas que pararam de responder através de mensagens personalizadas e relevantes.

INSTRUÇÕES ESPECÍFICAS:
- Use o contexto da conversa anterior para personalizar mensagens
- Seja natural e não robótico
- Mantenha o tom ${agent.tone || 'profissional'}
- Mencione algo específico da conversa se relevante
- Máximo 150 caracteres por mensagem
- Não use emojis em excesso

CONTATO: ${context.contactName}
TELEFONE: ${context.contactPhone}

Você está trabalhando no modo FOLLOW-UP AUTOMÁTICO.`;
}

/**
 * ✅ NOVO: Adicionar mensagem à thread OpenAI
 */
async function addMessageToOpenAIThread(apiKey, threadId, content, role) {
  try {
    const response = await axios.post(`https://api.openai.com/v1/threads/${threadId}/messages`, {
      role: role,
      content: content
    }, {
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'OpenAI-Beta': 'assistants=v2'
      },
      timeout: CONFIG.defaultResponseTimeoutMs
    });
    
    log('debug', `Mensagem ${role} adicionada à thread`, { threadId });
    
  } catch (error) {
    log('error', 'Erro ao adicionar mensagem à thread', { error: error.message });
    throw error;
  }
}

/**
 * ✅ NOVO: Executar run OpenAI com assistant
 */
async function executeOpenAIRun(apiKey, threadId, agent, conversationId, contactPhone) {
  try {
    log('debug', 'Iniciando run OpenAI com assistant');
    
    const runPayload = {
      assistant_id: agent.openai_assistant_id
    };
    
    const runResponse = await axios.post(`https://api.openai.com/v1/threads/${threadId}/runs`, runPayload, {
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'OpenAI-Beta': 'assistants=v2'
      },
      timeout: CONFIG.defaultResponseTimeoutMs
    });
    
    const runId = runResponse.data.id;
    let status = runResponse.data.status;
    
    log('debug', 'Run iniciado', { runId, status });
    
    // Poll para completar
    let attempts = 0;
    const maxAttempts = 30;
    
    while (status !== 'completed' && status !== 'failed' && attempts < maxAttempts) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      attempts++;
      
      const statusResponse = await axios.get(`https://api.openai.com/v1/threads/${threadId}/runs/${runId}`, {
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'OpenAI-Beta': 'assistants=v2'
        },
        timeout: CONFIG.defaultResponseTimeoutMs
      });
      
      status = statusResponse.data.status;
      log('debug', `Status do run: ${status} (tentativa ${attempts})`);
      
      if (status === 'failed') {
        return { 
          success: false, 
          error: statusResponse.data.last_error?.message || 'Run falhou' 
        };
      }
    }
    
    if (status === 'completed') {
      // Buscar dados finais do run para pegar usage
      const finalResponse = await axios.get(`https://api.openai.com/v1/threads/${threadId}/runs/${runId}`, {
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'OpenAI-Beta': 'assistants=v2'
        },
        timeout: CONFIG.defaultResponseTimeoutMs
      });
      
      return { 
        success: true, 
        usage: finalResponse.data.usage 
      };
    } else {
      return { 
        success: false, 
        error: 'Timeout ou falha no run' 
      };
    }
    
  } catch (error) {
    log('error', 'Erro ao executar run OpenAI', { error: error.message });
    return { success: false, error: error.message };
  }
}

/**
 * ✅ NOVO: Executar run OpenAI com modelo direto
 */
async function executeOpenAIRunWithModel(apiKey, threadId, agent, conversationId) {
  try {
    log('debug', 'Iniciando run OpenAI com modelo direto');
    
    const runPayload = {
      model: agent.openai_model || 'gpt-4o-mini',
      temperature: agent.temperature ?? 0.7,
      max_tokens: agent.max_tokens || 200,
      instructions: buildSystemPromptForFollowUp(agent, {
        contactName: 'Cliente',
        contactPhone: ''
      })
    };
    
    const runResponse = await axios.post(`https://api.openai.com/v1/threads/${threadId}/runs`, runPayload, {
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'OpenAI-Beta': 'assistants=v2'
      },
      timeout: CONFIG.defaultResponseTimeoutMs
    });
    
    const runId = runResponse.data.id;
    let status = runResponse.data.status;
    
    log('debug', 'Run com modelo iniciado', { runId, status, model: runPayload.model });
    
    // Poll para completar (mesmo processo)
    let attempts = 0;
    const maxAttempts = 30;
    
    while (status !== 'completed' && status !== 'failed' && attempts < maxAttempts) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      attempts++;
      
      const statusResponse = await axios.get(`https://api.openai.com/v1/threads/${threadId}/runs/${runId}`, {
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'OpenAI-Beta': 'assistants=v2'
        },
        timeout: CONFIG.defaultResponseTimeoutMs
      });
      
      status = statusResponse.data.status;
      log('debug', `Status do run modelo: ${status} (tentativa ${attempts})`);
      
      if (status === 'failed') {
        return { 
          success: false, 
          error: statusResponse.data.last_error?.message || 'Run com modelo falhou' 
        };
      }
    }
    
    if (status === 'completed') {
      // Buscar dados finais do run para pegar usage
      const finalResponse = await axios.get(`https://api.openai.com/v1/threads/${threadId}/runs/${runId}`, {
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'OpenAI-Beta': 'assistants=v2'
        },
        timeout: CONFIG.defaultResponseTimeoutMs
      });
      
      return { 
        success: true, 
        usage: finalResponse.data.usage 
      };
    } else {
      return { 
        success: false, 
        error: 'Timeout ou falha no run com modelo' 
      };
    }
    
  } catch (error) {
    log('error', 'Erro ao executar run com modelo', { error: error.message });
    return { success: false, error: error.message };
  }
}

/**
 * ✅ NOVO: Obter última mensagem do assistant da thread
 */
async function getLatestAssistantMessage(apiKey, threadId) {
  try {
    const response = await axios.get(`https://api.openai.com/v1/threads/${threadId}/messages?order=desc&limit=1`, {
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'OpenAI-Beta': 'assistants=v2'
      },
      timeout: CONFIG.defaultResponseTimeoutMs
    });
    
    const messages = response.data.data;
    
    for (const message of messages) {
      if (message.role === 'assistant' && message.content?.length > 0) {
        const textContent = message.content.find(c => c.type === 'text');
        if (textContent?.text?.value) {
          return textContent.text.value;
        }
      }
    }
    
    return null;
    
  } catch (error) {
    log('error', 'Erro ao obter mensagem do assistant', { error: error.message });
    return null;
  }
}



// ===============================================
// CORE: ENVIO VIA WHATSAPP
// ===============================================

/**
 * ✅ NOVO: Buscar configuração Evolution via ENV VARS (igual conversation.js)
 */
function getEvolutionConfig() {
  return {
    server_url: EVOLUTION_API_URL,
    api_key: EVOLUTION_API_KEY
  };
}

/**
 * Envia mensagem via WhatsApp usando ENV VARS da Evolution
 */
async function sendWhatsAppMessage(instanceName, recipientNumber, message) {
  try {
    // ✅ USAR ENV VARS diretamente (igual conversation.js)
    const evolutionConfig = getEvolutionConfig();
    
    log('debug', 'Enviando mensagem WhatsApp via ENV VARS', { 
      instanceName, 
      recipientNumber: recipientNumber.substring(0, 8) + '...',
      messageLength: message.length,
      serverUrl: evolutionConfig.server_url
    });
    
    const response = await axios.post(`${evolutionConfig.server_url}/message/sendText/${instanceName}`, {
      number: recipientNumber,
      text: message,
      options: {
        delay: 1000,
        presence: 'composing'
      }
    }, {
      headers: {
        'apikey': evolutionConfig.api_key,
        'Content-Type': 'application/json'
      },
      timeout: CONFIG.defaultResponseTimeoutMs
    });
    
    if (response.data.error) {
      throw new Error(response.data.message || 'Erro no envio WhatsApp');
    }
    
    log('success', 'Mensagem WhatsApp enviada via Evolution ENV', {
      instanceName,
      recipientNumber: recipientNumber.substring(0, 8) + '...',
      messageId: response.data.key?.id,
      serverUsed: evolutionConfig.server_url
    });
    
    return { success: true, messageId: response.data.key?.id };
    
  } catch (error) {
    log('error', 'Erro ao enviar WhatsApp via Evolution', { 
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
    log('info', `Processando follow-up: ${followUp.rule_name}`, { 
      followUpId: followUp.id,
      companyId: followUp.company_id,
      agentId: followUp.agent_id,
      conversationId: followUp.conversation_id
    });
    
    // ✅ PROTEÇÃO EXTRA: Verificar se follow-up ainda está pendente (evitar race conditions)
    const { data: currentStatus, error: statusError } = await supabase
      .from('follow_up_queue')
      .select('status, attempts')
      .eq('id', followUp.id)
      .single();
    
    if (statusError || !currentStatus) {
      throw new Error('Follow-up não encontrado ou já foi removido');
    }
    
    if (currentStatus.status !== 'pending') {
      log('warning', 'Follow-up não está mais pendente, pulando', { 
        followUpId: followUp.id,
        currentStatus: currentStatus.status,
        reason: 'status_changed_before_processing'
      });
      return { success: true, skipped: true, reason: 'status_changed' };
    }
    
    // ✅ PROTEÇÃO CONTRA MAX ATTEMPTS: Verificar se ainda pode tentar
    if (currentStatus.attempts >= followUp.max_attempts) {
      log('warning', 'Follow-up já atingiu máximo de tentativas', { 
        followUpId: followUp.id,
        currentAttempts: currentStatus.attempts,
        maxAttempts: followUp.max_attempts
      });
      
      // Marcar como failed
      await supabase
        .from('follow_up_queue')
        .update({ 
          status: 'failed',
          execution_error: `Máximo de ${followUp.max_attempts} tentativas atingido`
        })
        .eq('id', followUp.id);
        
      return { success: false, error: 'Max attempts reached' };
    }
    
    // ✅ VERIFICAÇÃO DE SEGURANÇA: Garantir que company_id existe
    if (!followUp.company_id) {
      log('error', 'Follow-up sem company_id - buscando do agente', { followUpId: followUp.id });
      
      // Buscar company_id do agente como fallback
      const { data: agent, error: agentError } = await supabase
        .from('ai_agents')
        .select('company_id')
        .eq('id', followUp.agent_id)
        .single();
        
      if (agentError || !agent?.company_id) {
        throw new Error(`Não foi possível determinar company_id para o follow-up ${followUp.id}`);
      }
      
      followUp.company_id = agent.company_id;
      log('info', 'Company_id recuperado do agente', { companyId: followUp.company_id });
    }
    
    // ✅ NOVO: Verificar créditos mínimos da empresa
    const creditsCheck = await checkCreditsBalance(followUp.company_id, CONFIG.credits.minimumBalanceThreshold);
    if (!creditsCheck.hasEnough) {
      // ✅ CORREÇÃO: Marcar como failed quando créditos insuficientes
      await supabase
        .from('follow_up_queue')
        .update({ 
          status: 'failed',
          attempts: followUp.attempts + 1,
          execution_error: `Créditos insuficientes (${creditsCheck.currentBalance}/${CONFIG.credits.minimumBalanceThreshold})`
        })
        .eq('id', followUp.id);
        
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
    
    if (rule?.conditions?.exclude_business_hours && !(await isBusinessHours(followUp.company_id))) {
      log('info', 'Follow-up adiado - fora do horário comercial da empresa', { 
        followUpId: followUp.id,
        companyId: followUp.company_id,
        timezone: await getCompanyTimezone(followUp.company_id)
      });
      
      // ✅ CORRIGIDO: Reagendar considerando timezone da empresa
      const timezone = await getCompanyTimezone(followUp.company_id);
      const nextBusinessHour = new Date();
      
      // Calcular próximo horário comercial no timezone da empresa
      const companyTime = new Intl.DateTimeFormat('en-CA', {
        timeZone: timezone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
      }).format(nextBusinessHour);
      
      const companyHour = parseInt(companyTime.split(' ')[1].split(':')[0]);
      
      // Se já passou do horário comercial hoje, agendar para amanhã
      if (companyHour >= CONFIG.businessHours.end) {
        nextBusinessHour.setDate(nextBusinessHour.getDate() + 1);
      }
      
      // Criar uma data no timezone da empresa para o próximo horário comercial
      const nextBusinessHourLocal = new Date(nextBusinessHour);
      nextBusinessHourLocal.setHours(CONFIG.businessHours.start, 0, 0, 0);
      
      await supabase
        .from('follow_up_queue')
        .update({ 
          scheduled_at: nextBusinessHourLocal.toISOString(),
          metadata: {
            ...followUp.metadata,
            rescheduled_reason: 'outside_business_hours',
            company_timezone: timezone,
            original_scheduled_at: followUp.scheduled_at
          }
        })
        .eq('id', followUp.id);
        
      return { success: true, deferred: true };
    }
    
    // 4. ✅ USAR APENAS MASTER KEY (simplificado)
    let finalMessage = followUp.message_template;
    
    log('info', 'Usando master key OpenAI para follow-up', { 
        companyId: followUp.company_id,
      agentName: agent.name
      });
      
      finalMessage = await generatePersonalizedMessageWithZionicCredits(
        followUp.message_template,
        context,
        agent,
        followUp.company_id
      );
      
    // ✅ Log de sucesso se personalizou (não é só replace)
      if (finalMessage && finalMessage !== followUp.message_template.replace('{nome}', context.contact?.first_name || 'usuário')) {
      log('debug', 'Follow-up personalizado com sucesso via master key + threads');
    }
    
    executionLog.message_sent = finalMessage;
    
    // 5. Buscar nome da instância WhatsApp (simplificado)
    const { data: instance, error: instanceError } = await supabase
      .from('whatsapp_instances')
      .select('name')
      .eq('company_id', followUp.company_id)
      .eq('status', 'connected')
      .single();
    
    if (instanceError || !instance?.name) {
      throw new Error('Instância WhatsApp ativa não encontrada');
    }
    
    // 6. Enviar mensagem via Evolution ENV VARS
    const sendResult = await sendWhatsAppMessage(
      instance.name,
      context.contact.phone,
      finalMessage
    );
    
    if (!sendResult.success) {
      throw new Error(sendResult.error);
    }
    
    // 7. ✅ CRÍTICO: Marcar como enviado com log detalhado
    log('debug', 'Marcando follow-up como enviado', { 
      followUpId: followUp.id, 
      currentStatus: 'pending', 
      newStatus: 'sent',
      conversationId: followUp.conversation_id,
      ruleId: followUp.rule_id
    });
    
    const { data: updateResult, error: updateError } = await supabase
      .from('follow_up_queue')
      .update({ 
        status: 'sent',
        attempts: followUp.attempts + 1,
        executed_at: new Date().toISOString(),
        ai_generated_message: finalMessage
      })
      .eq('id', followUp.id)
      .select('status, attempts');
    
    if (updateError) {
      log('error', 'ERRO CRÍTICO: Falha ao marcar follow-up como sent', { 
        followUpId: followUp.id,
        error: updateError.message,
        conversationId: followUp.conversation_id,
        ruleId: followUp.rule_id
      });
      throw new Error(`Erro ao marcar como sent: ${updateError.message}`);
    }
    
    log('success', 'Follow-up marcado como SENT com sucesso', { 
      followUpId: followUp.id,
      updateResult,
      conversationId: followUp.conversation_id,
      ruleId: followUp.rule_id,
      newStatus: updateResult?.[0]?.status
    });
    
    // 8. Registrar mensagem no sistema
    // ✅ CORRIGIDO: Usar mesmo formato do webhook para garantir compatibilidade com ChatWindow
    const messageData = {
      conversation_id: followUp.conversation_id,
      direction: 'outbound',
      message_type: 'text',
      content: finalMessage,
      from_number: context.contact.phone,
      from_name: agent.name,
      sent_at: new Date().toISOString(),
      status: 'sent', // ✅ OBRIGATÓRIO: Campo de status para compatibilidade
      sent_by_ai: true,
      external_id: null, // ✅ Campo para compatibilidade (follow-ups não têm ID externo)
      metadata: {
        follow_up_id: followUp.id,
        rule_name: followUp.rule_name,
        is_follow_up: true,
        sent_via: 'follow_up_server',
        instance_name: instance.name,
        ai_agent_id: followUp.agent_id, // ✅ CORRIGIDO: Agent ID vai no metadata
        agent_name: agent.name
      }
    };

    const { data: newMessage, error: messageError } = await supabase
      .from('messages')
      .insert(messageData)
      .select('id')
      .single();

    if (messageError) {
      log('error', 'Erro ao registrar mensagem no banco', { 
        error: messageError.message,
        messageData: { ...messageData, content: messageData.content.substring(0, 50) + '...' }
      });
      // Não falhar o follow-up por erro de log
    } else {
      log('success', 'Mensagem registrada no banco com sucesso', { 
        messageId: newMessage.id,
        isFollowUp: true,
        agentName: agent.name
      });
    }
    
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
 * ✅ OTIMIZADO: Limpa follow-ups antigos usando função SQL
 */
async function cleanupOldFailedFollowUps() {
  try {
    log('debug', 'Executando limpeza automática de follow-ups antigos...');
    
    const { data: cleanedCount, error } = await supabase.rpc('cleanup_old_follow_ups', {
      p_hours_old: 6
    });
    
    if (error) {
      log('warning', 'Erro ao limpar follow-ups antigos', { error: error.message });
      return;
    }
    
    if (cleanedCount && cleanedCount > 0) {
      log('info', `🧹 Limpeza automática: ${cleanedCount} follow-ups antigos marcados como failed`, {
        olderThan: '6 horas',
        method: 'sql_function'
      });
    }
    
  } catch (error) {
    log('error', 'Erro na limpeza de follow-ups antigos', { error: error.message });
  }
}

/**
 * ✅ OTIMIZADO: Detecta e cria follow-ups órfãos usando função SQL eficiente
 */
async function findAndCreateOrphanedFollowUps() {
  try {
    log('info', '🔍 Detectando follow-ups órfãos com SQL otimizado...');
    
    // ✅ DEBUG: Verificar se há follow-ups 'sent' que podem estar sendo ignorados
    const { data: sentFollowUps, error: sentError } = await supabase
      .from('follow_up_queue')
      .select('conversation_id, rule_id, status, scheduled_at, executed_at')
      .eq('status', 'sent')
      .gte('executed_at', new Date(Date.now() - 10 * 60 * 1000).toISOString()) // últimos 10 minutos
      .limit(10);
    
    if (!sentError && sentFollowUps?.length > 0) {
      log('debug', 'Follow-ups SENT encontrados (últimos 10min)', { 
        count: sentFollowUps.length,
        examples: sentFollowUps.map(f => ({
          conversationId: f.conversation_id,
          ruleId: f.rule_id,
          status: f.status,
          executedAt: f.executed_at
        }))
      });
    }
    
    // ✅ Limpeza automática antes da detecção
    await cleanupOldFailedFollowUps();
    
    // ✅ ULTRA SEGURO: Usar função SQL com verificação tripla
    const { data: orphanedFollowUps, error } = await supabase.rpc('create_orphaned_follow_ups_ultra_safe', {
      p_limit: 1000,  // Até 1000 follow-ups órfãos por execução
      p_days_back: 7   // Últimos 7 dias
    });

    if (error) {
      log('error', 'Erro na detecção SQL de órfãos', { error: error.message });
      return [];
    }

    if (!orphanedFollowUps || orphanedFollowUps.length === 0) {
      log('info', 'Nenhum follow-up órfão encontrado');
      return [];
    }

    // ✅ DEBUG CRÍTICO: Verificar se algum órfão criado tem conflito com follow-ups 'sent'
    for (const orphan of orphanedFollowUps.slice(0, 5)) { // Verificar apenas os primeiros 5
      const { data: existingSent, error: checkError } = await supabase
        .from('follow_up_queue')
        .select('id, status, executed_at')
        .eq('conversation_id', orphan.conversation_id)
        .eq('rule_id', orphan.rule_id)
        .eq('status', 'sent')
        .order('executed_at', { ascending: false })
        .limit(1);
      
      if (!checkError && existingSent?.length > 0) {
        log('error', '🚨 PROBLEMA CRÍTICO: Órfão criado para conversa que JÁ TEM follow-up SENT!', {
          orphanConversationId: orphan.conversation_id,
          orphanRuleId: orphan.rule_id,
          existingSentId: existingSent[0].id,
          existingSentExecutedAt: existingSent[0].executed_at,
          timeSinceSent: Date.now() - new Date(existingSent[0].executed_at).getTime()
        });
      }
    }

    log('success', `✅ Detecção SQL ULTRA SEGURA concluída: ${orphanedFollowUps.length} follow-ups órfãos criados`, {
      method: 'sql_ultra_safe_triple_verification',
      orphansCreated: orphanedFollowUps.length,
      averageLateness: orphanedFollowUps.reduce((acc, f) => acc + (f.minutes_late || 0), 0) / orphanedFollowUps.length,
      fixVersion: '3.0_ultra_safe'
    });

    // Log dos órfãos criados para debug
    if (orphanedFollowUps.length <= 10) {
      orphanedFollowUps.forEach(followUp => {
        log('debug', 'Follow-up órfão criado', {
          conversationId: followUp.conversation_id,
          ruleName: followUp.rule_name,
          minutesLate: followUp.minutes_late,
          scheduledAt: followUp.scheduled_at
        });
      });
    }

    return orphanedFollowUps;

  } catch (error) {
    log('error', 'Erro na detecção otimizada de órfãos', { error: error.message });
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
      version: '1.6.3', // ✅ CORREÇÃO DEFINITIVA: Loop infinito por trigger corrigido
      uptime: formatDuration(Date.now() - stats.serverStartTime),
      stats: {
        ...stats,
        nextExecution: 'A cada 1 minuto (máxima precisão)'
      },
      features: {
        orphanSync: true,
        creditsControl: true,
        intelligentFallback: true, // ✅ NOVO: Fallback inteligente OpenAI
        systemNotifications: true, // ✅ NOVO: Notificações automáticas
        loopPrevention: true, // ✅ NOVO: Prevenção de loop infinito
        persistentThreads: true, // ✅ NOVO: Threads persistentes OpenAI
        masterKeyOnly: true, // ✅ NOVO: Apenas master key (sem chave própria)
        intervalMinutes: CONFIG.executionIntervalMinutes
      },
      fixes: {
        v163: 'CORREÇÃO DEFINITIVA: Loop infinito causado pelo trigger de follow-up',
        triggerLoopFix: 'Trigger ignora mensagens enviadas pelo follow-up server',
        metadataExclusion: 'Mensagens com is_follow_up=true são ignoradas pelo trigger',
        autoCleanup: 'Limpeza automática de follow-ups duplicados/órfãos',
        timezoneCorrect: 'Horário comercial baseado no timezone da empresa',
        threadsConsistency: 'Threads persistentes igual webhook principal',
        masterKeyOnly: 'Removidas chaves próprias - apenas master key'
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

// ===============================================
// ✅ NOVO: SISTEMA DE NOTIFICAÇÕES
// ===============================================

/**
 * Cria uma notificação no sistema para avisar sobre problemas
 */
async function createSystemNotification(companyId, type, title, message, severity = 'medium', metadata = {}) {
  try {
    if (!companyId || companyId === 'undefined' || companyId === null) {
      log('error', 'CompanyId inválido para criar notificação', { companyId, type });
      return;
    }

    log('debug', 'Criando notificação no sistema', { 
      companyId: companyId.toString(), 
      type, 
      title, 
      severity 
    });

    const { data, error } = await supabase
      .from('system_notifications')
      .insert({
        company_id: companyId,
        type: type,
        title: title,
        message: message,
        severity: severity,
        metadata: metadata,
        is_read: false
      })
      .select('id')
      .single();

    if (error) {
      log('error', 'Erro ao criar notificação', { 
        error: error.message, 
        companyId: companyId.toString(),
        type 
      });
      return;
    }

    log('success', 'Notificação criada com sucesso', { 
      companyId: companyId.toString(), 
      notificationId: data.id,
      type,
      severity 
    });

    return data.id;

  } catch (error) {
    log('error', 'Erro ao criar notificação do sistema', { 
      error: error.message, 
      companyId: companyId ? companyId.toString() : 'null',
      type 
    });
  }
}



/**
 * Cria notificação quando Zionic Credits estão insuficientes
 */
async function notifyZionicCreditsInsufficient(companyId, currentBalance, required) {
  const title = '💰 Zionic Credits Insuficientes';
  const message = `Seus Zionic Credits estão baixos (${currentBalance} disponíveis, ${required} necessários). Compre mais créditos para manter os follow-ups funcionando com IA.`;
  
  const metadata = {
    current_balance: currentBalance,
    required_credits: required,
    purchase_url: '/settings?subtab=integracoes#credits',
    timestamp: new Date().toISOString(),
    fallback_to_template: true
  };

  await createSystemNotification(
    companyId,
    'zionic_credits_insufficient',
    title,
    message,
    'medium',
    metadata
  );
}

/**
 * Cria notificação quando há erro crítico com OpenAI
 */
async function notifyOpenAIError(companyId, errorDetails, mode = 'master_key') {
  const title = '⚠️ Erro no Sistema IA';
  const message = 'Erro temporário no sistema de IA. Follow-ups usarão templates simples até resolver.';
  
  const metadata = {
    error_message: errorDetails.message || 'Erro desconhecido',
    error_code: errorDetails.status || 500,
    system: 'master_key_threads',
    timestamp: new Date().toISOString(),
    fallback_active: true
  };

  await createSystemNotification(
    companyId,
    'master_key_error',
    title,
    message,
    'high',
    metadata
  );
}

 
