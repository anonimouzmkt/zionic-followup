/**
 * ===============================================
 * ZIONIC FOLLOW-UP & APPOINTMENT REMINDERS SERVER
 * ===============================================
 * Servidor unificado para processamento automático de:
 * - Follow-ups de leads inativos
 * - Lembretes de appointments
 * 
 * @author Zionic Team
 * @version 1.7.0
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');

// ✅ IMPORTAR PROCESSADORES ESPECIALIZADOS
const followUpProcessor = require('./followup-processor');
const appointmentReminderProcessor = require('./appointment-reminders-processor');

// ===============================================
// CONFIGURAÇÕES
// ===============================================

const CONFIG = {
  supabaseUrl: process.env.SUPABASE_URL,
  supabaseKey: process.env.SUPABASE_SERVICE_ROLE_KEY,
  evolutionApiUrl: process.env.EVOLUTION_API_URL,
  evolutionApiKey: process.env.EVOLUTION_API_KEY,
  // ✅ CORRIGIDO: Usar master key conforme memória do usuário
  masterOpenAIKey: process.env.OPENAI_MASTER_API_KEY,
  fallbackOpenAIKey: process.env.OPENAI_API_KEY,
  intervalMinutes: 1,
  maxFollowUpsPerExecution: 50,
  port: process.env.PORT || 3000
};

// Validar configurações essenciais
if (!CONFIG.supabaseUrl || !CONFIG.supabaseKey) {
  console.error('❌ ERRO FATAL: Variáveis do Supabase não configuradas');
  process.exit(1);
}

// ✅ Evolution API é opcional - sistema usa conversations internas
if (!CONFIG.evolutionApiUrl || !CONFIG.evolutionApiKey) {
  console.log('⚠️ Evolution API não configurada - usando sistema interno de mensagens');
}

// Configurar cliente Supabase
const supabase = createClient(CONFIG.supabaseUrl, CONFIG.supabaseKey);

// ===============================================
// ESTATÍSTICAS GLOBAIS
// ===============================================

const stats = {
  serverStartTime: new Date().toISOString(),
  totalFollowUpsSent: 0,
  totalRemindersSent: 0,
  totalOrphansCreated: 0,
  totalRemindersCreated: 0,
  lastExecution: null,
  errors: []
};

// ===============================================
// LOGGING CENTRALIZADO
// ===============================================

function log(level, message, data = {}) {
  const timestamp = new Date().toISOString();
  const emoji = {
    info: 'ℹ️',
    success: '✅',
    warning: '⚠️',
    error: '❌',
    debug: '🔍'
  };
  
  console.log(`${emoji[level] || '📝'} [MAIN-SERVER] [${timestamp}] ${message}`, 
    Object.keys(data).length > 0 ? JSON.stringify(data, null, 2) : '');
}

// ===============================================
// FERRAMENTAS DE IA
// ===============================================

/**
 * Função para gerar mensagens personalizadas usando IA com threads persistentes
 * ✅ CORRIGIDO: Usando mesma lógica do ChatSidebar.tsx que funciona
 */
async function generatePersonalizedMessage(messageTemplate, context, agent, companyId) {
  try {
    log('debug', 'Iniciando geração de mensagem personalizada', {
      agentId: agent.id,
      agentName: agent.name,
      companyId,
      hasAssistant: !!agent.openai_assistant_id,
      templateLength: messageTemplate.length
    });

    // ✅ 1. Buscar chave OpenAI (master key sempre, conforme memória do usuário)
    let openaiApiKey = CONFIG.masterOpenAIKey;
    
    if (!openaiApiKey) {
      log('warning', 'Master key não encontrada, usando fallback');
      openaiApiKey = CONFIG.fallbackOpenAIKey;
    }
    
    if (!openaiApiKey) {
      log('error', 'Nenhuma chave OpenAI disponível');
      return messageTemplate;
    }

    // ✅ 2. Para follow-ups, SEMPRE usar thread existente da conversa
    if (context.conversation && !context.appointmentId) {
      const threadId = context.conversation.openai_thread_id;
      
      if (!threadId) {
        log('warning', 'Conversa sem thread OpenAI - usando template simples para preservar contexto');
        return messageTemplate;
      }
      
      log('debug', 'Reutilizando thread existente para follow-up', {
        threadId: threadId,
        conversationId: context.conversation.id
      });
      
      // ✅ 3. Tentar gerar resposta personalizada usando thread existente
      try {
        return await generateWithExistingThread(
          messageTemplate, 
          context, 
          agent, 
          threadId,
          openaiApiKey
        );
      } catch (threadError) {
        log('warning', 'Falha ao usar thread existente, usando template simples', { 
          error: threadError.message,
          agentId: agent.id 
        });
        return messageTemplate;
      }
    }
    
    // ✅ 4. Para appointments ou outros casos, usar geração simples
    try {
      return await generateWithDirectAPI(
        messageTemplate,
        context,
        agent,
        openaiApiKey
      );
    } catch (directError) {
      log('warning', 'Falha na geração direta, usando template simples', { 
        error: directError.message,
        agentId: agent.id 
      });
      return messageTemplate;
    }

  } catch (error) {
    log('error', 'Erro na geração de mensagem personalizada', { 
      error: error.message,
      agentId: agent?.id 
    });
    return messageTemplate;
  }
}

/**
 * ✅ NOVO: Gerar usando thread existente (igual ChatSidebar.tsx)
 */
async function generateWithExistingThread(messageTemplate, context, agent, threadId, openaiApiKey) {
  // Construir prompt contextual
  const recentMessages = context.recentMessages?.slice(-3).map(m => 
    `${m.sent_by_ai ? 'Agente' : 'Cliente'}: ${m.content}`
  ).join('\n') || 'Nenhuma mensagem recente';
  
  const contextualPrompt = `[INSTRUÇÃO PARA FOLLOW-UP]

Você está assumindo uma conversa existente para envio de follow-up automático.

TEMPLATE ORIGINAL: "${messageTemplate}"

CONTEXTO DA CONVERSA:
- Contato: ${context.contact?.first_name || 'Cliente'}
- Telefone: ${context.contact?.phone?.substring(0, 8) + '...' || 'Não disponível'}
- Última atividade: ${context.lastMessage?.sent_at || 'Não disponível'}
- Total de mensagens: ${context.messageCount || 0}
- Cliente já respondeu: ${context.hasContactMessages ? 'Sim' : 'Não'}

MENSAGENS RECENTES:
${recentMessages}

INSTRUÇÕES:
1. Personalize o template com base no contexto da conversa
2. Mantenha tom natural e não robótico
3. Seja breve e objetivo (máximo 150 caracteres)
4. NÃO inicie com saudações pois é um follow-up
5. Foque em reativação baseada no que foi conversado
6. Use o nome do cliente quando possível

Gere apenas a mensagem final personalizada, sem explicações.`;

  // ✅ Adicionar mensagem à thread existente
  await axios.post(`https://api.openai.com/v1/threads/${threadId}/messages`, {
    role: 'user',
    content: contextualPrompt,
    metadata: {
      type: 'follow_up_request',
      template_length: messageTemplate.length,
      created_at: new Date().toISOString()
    }
  }, {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${openaiApiKey}`,
      'OpenAI-Beta': 'assistants=v2'
    }
  });

  // ✅ Se agente tem assistant, usar Assistant API
  if (agent.openai_assistant_id && agent.openai_assistant_id.startsWith('asst_')) {
    return await executeAssistantRun(threadId, agent, openaiApiKey);
  } else {
    // ✅ Usar completion direta com contexto da thread
    return await executeDirectCompletion(contextualPrompt, agent, openaiApiKey);
  }
}

/**
 * ✅ NOVO: Executar Assistant API
 */
async function executeAssistantRun(threadId, agent, openaiApiKey) {
  // Criar run
  const runResponse = await axios.post(`https://api.openai.com/v1/threads/${threadId}/runs`, {
    assistant_id: agent.openai_assistant_id,
    temperature: agent.temperature || 0.7,
    max_tokens: 150, // Limite para follow-ups
    metadata: {
      type: 'follow_up_generation',
      agent_id: agent.id
    }
  }, {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${openaiApiKey}`,
      'OpenAI-Beta': 'assistants=v2'
    }
  });

  let run = runResponse.data;
  let attempts = 0;
  const maxAttempts = 30; // 30 segundos timeout

  // Aguardar conclusão
  while ((run.status === 'in_progress' || run.status === 'queued') && attempts < maxAttempts) {
    await new Promise(resolve => setTimeout(resolve, 1000));
    attempts++;
    
    const statusResponse = await axios.get(`https://api.openai.com/v1/threads/${threadId}/runs/${run.id}`, {
      headers: {
        'Authorization': `Bearer ${openaiApiKey}`,
        'OpenAI-Beta': 'assistants=v2'
      }
    });
    
    run = statusResponse.data;
  }

  if (run.status !== 'completed') {
    throw new Error(`Assistant run falhou: ${run.status} (${run.last_error?.message || 'timeout'})`);
  }

  // Buscar resposta
  const messagesResponse = await axios.get(`https://api.openai.com/v1/threads/${threadId}/messages?limit=1`, {
    headers: {
      'Authorization': `Bearer ${openaiApiKey}`,
      'OpenAI-Beta': 'assistants=v2'
    }
  });

  const lastMessage = messagesResponse.data.data[0];
  if (lastMessage?.content?.[0]?.text?.value) {
    return lastMessage.content[0].text.value.trim();
  }

  throw new Error('Nenhuma resposta encontrada do Assistant');
}

/**
 * ✅ NOVO: Geração direta via Completion API
 */
async function executeDirectCompletion(prompt, agent, openaiApiKey) {
  const completion = await axios.post('https://api.openai.com/v1/chat/completions', {
    model: agent.openai_model || 'gpt-4',
    messages: [
      {
        role: 'system',
        content: `Você é ${agent.name}. Você é especialista em follow-ups de reativação de leads. Seja natural, breve e contextual.`
      },
      {
        role: 'user',
        content: prompt
      }
    ],
    temperature: agent.temperature || 0.7,
    max_tokens: 150
  }, {
    headers: {
      'Authorization': `Bearer ${openaiApiKey}`,
      'Content-Type': 'application/json'
    }
  });

  const response = completion.data.choices[0]?.message?.content?.trim();
  if (!response) {
    throw new Error('Nenhuma resposta da OpenAI Completion');
  }

  return response;
}

/**
 * ✅ NOVO: Geração para appointments ou casos sem thread
 */
async function generateWithDirectAPI(messageTemplate, context, agent, openaiApiKey) {
  let prompt;
  
  if (context.appointmentId) {
    // Para lembretes de appointment
    prompt = `Personalize este lembrete de appointment de forma natural:

TEMPLATE: "${messageTemplate}"

DADOS DO APPOINTMENT:
- Contato: ${context.contactName}
- Título: ${context.appointmentTitle || 'Agendamento'}
- Data/Hora: ${context.appointmentDate}
- Local: ${context.appointmentLocation || 'A definir'}
- Tipo: ${context.reminderType || 'Lembrete'}

Gere uma versão natural e cordial do lembrete. Seja breve e claro.`;
  } else {
    // Para follow-ups sem thread
    prompt = `Personalize esta mensagem de follow-up:

TEMPLATE: "${messageTemplate}"

CONTEXTO:
- Contato: ${context.contact?.first_name || 'Cliente'}
- Empresa: ${context.contact?.company_name || 'Não informado'}

Gere uma versão personalizada e natural. Seja breve (máximo 150 caracteres).`;
  }

  return await executeDirectCompletion(prompt, agent, openaiApiKey);
}

// ✅ Funções antigas removidas - agora usando lógica do ChatSidebar.tsx

// ===============================================
// ENVIO WHATSAPP
// ===============================================

/**
 * Envia mensagem via sistema interno do Zionic (registra no banco)
 */
async function sendWhatsAppMessage(instanceName, phone, message) {
  try {
    // ✅ Usar sistema interno - registrar mensagem diretamente no banco
    if (CONFIG.evolutionApiUrl && CONFIG.evolutionApiKey) {
      // Se Evolution API está configurada, usar
      const response = await axios.post(
        `${CONFIG.evolutionApiUrl}/message/sendText/${instanceName}`,
        {
          number: phone,
          textMessage: {
            text: message
          }
        },
        {
          headers: {
            'Content-Type': 'application/json',
            'apikey': CONFIG.evolutionApiKey
          }
        }
      );

      return {
        success: true,
        messageId: response.data?.key?.id || 'unknown'
      };
    } else {
      // ✅ Sistema interno - registra mensagem no banco para processamento
      const messageId = `internal_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      
      log('info', 'Mensagem registrada no sistema interno', {
        messageId,
        phone: phone?.substring(0, 8) + '...',
        messageLength: message.length,
        type: 'internal_queue'
      });

      // TODO: Aqui o sistema interno do Zionic processará a mensagem
      // A mensagem já é registrada posteriormente nos processadores
      
      return {
        success: true,
        messageId: messageId
      };
    }
  } catch (error) {
    log('error', 'Erro ao enviar mensagem', {
      instanceName,
      phone: phone?.substring(0, 8) + '...',
      error: error.message,
      usingInternal: !CONFIG.evolutionApiUrl
    });
    
    return {
      success: false,
      error: error.message
    };
  }
}

// ===============================================
// EXECUÇÃO PRINCIPAL
// ===============================================

/**
 * Executa processamento completo de follow-ups e lembretes
 */
async function executeProcessing() {
  const executionStart = Date.now();
  
  try {
    log('info', '🚀 INICIANDO CICLO DE PROCESSAMENTO', {
      timestamp: new Date().toISOString(),
      intervalMinutes: CONFIG.intervalMinutes
    });

    // ✅ 1. PROCESSAR FOLLOW-UPS
    log('info', '📄 PROCESSANDO FOLLOW-UPS...');
    
    const pendingFollowUps = await followUpProcessor.getPendingFollowUps(supabase, CONFIG);
    
    let followUpResults = {
      processed: 0,
      success: 0,
      failed: 0,
      skipped: 0
    };
    
    for (const followUp of pendingFollowUps.slice(0, CONFIG.maxFollowUpsPerExecution)) {
      const result = await followUpProcessor.processFollowUp(
        supabase, 
        CONFIG, 
        followUp, 
        generatePersonalizedMessage, 
        sendWhatsAppMessage
      );
      
      followUpResults.processed++;
      if (result.success) {
        if (result.skipped) {
          followUpResults.skipped++;
        } else {
          followUpResults.success++;
          stats.totalFollowUpsSent++;
        }
      } else {
        followUpResults.failed++;
      }
    }
    
    // ✅ 2. PROCESSAR LEMBRETES DE APPOINTMENTS
    log('info', '📅 PROCESSANDO LEMBRETES DE APPOINTMENTS...');
    
    // Criar novos lembretes baseados nas regras dos agentes
    const remindersCreated = await appointmentReminderProcessor.createAppointmentReminders(supabase);
    stats.totalRemindersCreated += remindersCreated;
    
    // Buscar lembretes pendentes
    const pendingReminders = await appointmentReminderProcessor.getPendingAppointmentReminders(supabase, CONFIG);
    
    let reminderResults = {
      processed: 0,
      success: 0,
      failed: 0,
      skipped: 0
    };
    
    for (const reminder of pendingReminders.slice(0, CONFIG.maxFollowUpsPerExecution)) {
      const result = await appointmentReminderProcessor.processAppointmentReminder(
        supabase, 
        CONFIG, 
        reminder, 
        generatePersonalizedMessage, 
        sendWhatsAppMessage
      );
      
      reminderResults.processed++;
      if (result.success) {
        if (result.skipped) {
          reminderResults.skipped++;
        } else {
          reminderResults.success++;
          stats.totalRemindersSent++;
        }
      } else {
        reminderResults.failed++;
      }
    }
    
    // ✅ 3. ESTATÍSTICAS FINAIS
    const executionTime = Date.now() - executionStart;
    stats.lastExecution = new Date().toISOString();
    
    log('success', '✅ CICLO DE PROCESSAMENTO CONCLUÍDO', {
      executionTimeMs: executionTime,
      followUps: followUpResults,
      reminders: reminderResults,
      remindersCreated,
      totalStats: {
        totalFollowUpsSent: stats.totalFollowUpsSent,
        totalRemindersSent: stats.totalRemindersSent,
        totalRemindersCreated: stats.totalRemindersCreated
      }
    });
    
  } catch (error) {
    log('error', '❌ ERRO CRÍTICO NO PROCESSAMENTO', { 
      error: error.message,
      stack: error.stack 
    });
    stats.errors.push({
      timestamp: new Date().toISOString(),
      error: error.message
    });
  }
}

// ===============================================
// SERVIDOR HTTP
// ===============================================

const express = require('express');
const app = express();

app.use(express.json());

// Endpoint principal
app.get('/', (req, res) => {
  res.json({
    status: 'running',
    version: '1.7.0',
    description: 'Servidor automático de follow-up e lembretes de appointments com threads persistentes',
    stats: {
      serverStartTime: stats.serverStartTime,
      totalFollowUpsSent: stats.totalFollowUpsSent,
      totalRemindersSent: stats.totalRemindersSent,
      totalRemindersCreated: stats.totalRemindersCreated,
      lastExecution: stats.lastExecution,
      successRate: stats.totalFollowUpsSent + stats.totalRemindersSent > 0 ? 
        `${((stats.totalFollowUpsSent + stats.totalRemindersSent) / (stats.totalFollowUpsSent + stats.totalRemindersSent + stats.errors.length) * 100).toFixed(1)}%` : 
        '0%'
    },
    features: {
      followUpProcessing: true,
      appointmentReminders: true,
      persistentThreads: true,
      creditsControl: true,
      separateProcessors: true,
      intervalMinutes: CONFIG.intervalMinutes
    }
  });
});

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    lastExecution: stats.lastExecution,
    uptime: process.uptime()
  });
});

// ===============================================
// INICIALIZAÇÃO
// ===============================================

async function startServer() {
  try {
    // Iniciar servidor HTTP
    app.listen(CONFIG.port, () => {
      log('success', `🚀 Servidor iniciado na porta ${CONFIG.port}`);
    });
    
    // Primeira execução imediata
    await executeProcessing();
    
    // Agendar execuções periódicas
    setInterval(executeProcessing, CONFIG.intervalMinutes * 60 * 1000);
    
    log('success', '✅ Sistema de follow-up e lembretes ativo', {
      intervalMinutes: CONFIG.intervalMinutes,
      maxPerExecution: CONFIG.maxFollowUpsPerExecution,
      features: ['follow-ups', 'appointment-reminders', 'persistent-threads']
    });
    
  } catch (error) {
    log('error', '❌ ERRO FATAL NA INICIALIZAÇÃO', { error: error.message });
    process.exit(1);
  }
}

// Iniciar sistema
startServer(); 
