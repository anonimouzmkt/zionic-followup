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
  zionicOpenAIKey: process.env.ZIONIC_OPENAI_KEY,
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

    // ✅ 1. Buscar ou criar thread OpenAI para a conversa/lembrete
    const threadId = await getOrCreateThread(context, agent, companyId);
    
    if (!threadId) {
      log('warning', 'Não foi possível obter thread, usando template simples');
      return messageTemplate;
    }

    // ✅ 2. Tentar com Assistant + Thread (se agente tem assistant)
    if (agent.openai_assistant_id && agent.openai_assistant_id.startsWith('asst_')) {
      try {
        return await generateWithAssistantAndThread(
          messageTemplate, 
          context, 
          agent, 
          threadId,
          companyId
        );
      } catch (assistantError) {
        log('warning', 'Falha no Assistant, tentando com Thread direta', { 
          error: assistantError.message,
          agentId: agent.id 
        });
      }
    }

    // ✅ 3. Fallback: Thread + modelo direto
    try {
      return await generateWithThreadOnly(
        messageTemplate, 
        context, 
        agent, 
        threadId,
        companyId
      );
    } catch (threadError) {
      log('warning', 'Falha na Thread, usando template simples', { 
        error: threadError.message,
        agentId: agent.id 
      });
    }

    // ✅ 4. Fallback final: template com substituições básicas
    return messageTemplate;

  } catch (error) {
    log('error', 'Erro na geração de mensagem personalizada', { 
      error: error.message,
      agentId: agent?.id 
    });
    return messageTemplate;
  }
}

/**
 * Obter ou criar thread OpenAI
 */
async function getOrCreateThread(context, agent, companyId) {
  try {
    const openaiKey = await getOpenAIKey(companyId);
    
    // Para lembretes, criar thread nova (contexto específico do appointment)
    if (context.appointmentId) {
      log('debug', 'Criando nova thread para lembrete de appointment', {
        appointmentId: context.appointmentId,
        agentId: agent.id
      });
      
      const response = await axios.post('https://api.openai.com/v1/threads', {}, {
        headers: {
          'Authorization': `Bearer ${openaiKey}`,
          'Content-Type': 'application/json',
          'OpenAI-Beta': 'assistants=v2'
        }
      });
      
      return response.data.id;
    }
    
    // Para follow-ups, buscar thread existente da conversa
    if (context.conversation) {
      const existingThreadId = context.conversation.metadata?.openai_thread_id;
      
      if (existingThreadId) {
        log('debug', 'Reutilizando thread existente para follow-up', {
          threadId: existingThreadId,
          conversationId: context.conversation.id
        });
        return existingThreadId;
      }
      
      // Criar nova thread para a conversa
      log('debug', 'Criando nova thread para follow-up', {
        conversationId: context.conversation.id
      });
      
      const response = await axios.post('https://api.openai.com/v1/threads', {}, {
        headers: {
          'Authorization': `Bearer ${openaiKey}`,
          'Content-Type': 'application/json',
          'OpenAI-Beta': 'assistants=v2'
        }
      });
      
      // Salvar thread ID na conversa
      await supabase
        .from('conversations')
        .update({
          metadata: {
            ...context.conversation.metadata,
            openai_thread_id: response.data.id
          }
        })
        .eq('id', context.conversation.id);
      
      return response.data.id;
    }
    
    return null;
    
  } catch (error) {
    log('error', 'Erro ao obter/criar thread OpenAI', { error: error.message });
    return null;
  }
}

/**
 * Gerar com Assistant + Thread
 */
async function generateWithAssistantAndThread(messageTemplate, context, agent, threadId, companyId) {
  const openaiKey = await getOpenAIKey(companyId);
  
  // Adicionar mensagem do sistema à thread
  await addSystemMessageToThread(threadId, messageTemplate, context, agent, openaiKey);
  
  // Executar assistant na thread
  const runResponse = await axios.post(`https://api.openai.com/v1/threads/${threadId}/runs`, {
    assistant_id: agent.openai_assistant_id,
    temperature: agent.temperature || 0.7,
    max_tokens: agent.max_tokens || 500
  }, {
    headers: {
      'Authorization': `Bearer ${openaiKey}`,
      'Content-Type': 'application/json',
      'OpenAI-Beta': 'assistants=v2'
    }
  });
  
  // Aguardar conclusão
  let run = runResponse.data;
  while (run.status === 'in_progress' || run.status === 'queued') {
    await new Promise(resolve => setTimeout(resolve, 1000));
    const statusResponse = await axios.get(`https://api.openai.com/v1/threads/${threadId}/runs/${run.id}`, {
      headers: {
        'Authorization': `Bearer ${openaiKey}`,
        'OpenAI-Beta': 'assistants=v2'
      }
    });
    run = statusResponse.data;
  }
  
  if (run.status !== 'completed') {
    throw new Error(`Run falhou com status: ${run.status}`);
  }
  
  // Buscar resposta
  const messagesResponse = await axios.get(`https://api.openai.com/v1/threads/${threadId}/messages`, {
    headers: {
      'Authorization': `Bearer ${openaiKey}`,
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
 * Gerar com Thread + modelo direto
 */
async function generateWithThreadOnly(messageTemplate, context, agent, threadId, companyId) {
  const openaiKey = await getOpenAIKey(companyId);
  
  // Adicionar mensagem do usuário à thread
  await axios.post(`https://api.openai.com/v1/threads/${threadId}/messages`, {
    role: 'user',
    content: buildPrompt(messageTemplate, context, agent)
  }, {
    headers: {
      'Authorization': `Bearer ${openaiKey}`,
      'Content-Type': 'application/json',
      'OpenAI-Beta': 'assistants=v2'
    }
  });
  
  // Chamar completion diretamente
  const completion = await axios.post('https://api.openai.com/v1/chat/completions', {
    model: agent.openai_model || 'gpt-4',
    messages: [
      {
        role: 'system',
        content: buildSystemPrompt(agent, context)
      },
      {
        role: 'user',
        content: buildPrompt(messageTemplate, context, agent)
      }
    ],
    temperature: agent.temperature || 0.7,
    max_tokens: agent.max_tokens || 500
  }, {
    headers: {
      'Authorization': `Bearer ${openaiKey}`,
      'Content-Type': 'application/json'
    }
  });
  
  const response = completion.data.choices[0]?.message?.content?.trim();
  if (!response) {
    throw new Error('Nenhuma resposta da OpenAI');
  }
  
  return response;
}

/**
 * Adicionar mensagem do sistema à thread
 */
async function addSystemMessageToThread(threadId, messageTemplate, context, agent, openaiKey) {
  const systemPrompt = buildSystemPrompt(agent, context);
  const userPrompt = buildPrompt(messageTemplate, context, agent);
  
  await axios.post(`https://api.openai.com/v1/threads/${threadId}/messages`, {
    role: 'user',
    content: `${systemPrompt}\n\n${userPrompt}`
  }, {
    headers: {
      'Authorization': `Bearer ${openaiKey}`,
      'Content-Type': 'application/json',
      'OpenAI-Beta': 'assistants=v2'
    }
  });
}

/**
 * Construir prompt do sistema
 */
function buildSystemPrompt(agent, context) {
  if (context.appointmentId) {
    return `Você é ${agent.name}, um assistente especializado em lembretes de appointments.
    
Sua função: Personalizar lembretes de appointments de forma natural e empática.

Diretrizes:
- Mantenha tom profissional e cordial
- Inclua informações relevantes do appointment
- Seja claro sobre data, horário e local
- Use linguagem natural e acolhedora
- Evite ser muito formal ou robótico`;
  } else {
    return agent.system_prompt || `Você é ${agent.name}, um assistente de follow-up.
    
Sua função: Gerar mensagens de follow-up personalizadas para reativação de leads.

Diretrizes:
- Personalize com base no contexto da conversa
- Mantenha tom natural e não invasivo
- Foque em reativação sem ser insistente
- Use informações do contato quando disponível`;
  }
}

/**
 * Construir prompt da mensagem
 */
function buildPrompt(messageTemplate, context, agent) {
  if (context.appointmentId) {
    return `Personalize este lembrete de appointment:
    
Template: "${messageTemplate}"

Dados do appointment:
- Contato: ${context.contactName}
- Título: ${context.appointmentTitle}
- Data/Hora: ${context.appointmentDate}
- Local: ${context.appointmentLocation || 'Não especificado'}
- Tipo de lembrete: ${context.reminderType}
- Minutos antes: ${context.minutesBefore}

Gere uma versão personalizada e natural do lembrete.`;
  } else {
    const recentMessages = context.recentMessages?.slice(-3).map(m => 
      `${m.sent_by_ai ? 'Agente' : 'Cliente'}: ${m.content}`
    ).join('\n') || 'Nenhuma mensagem recente';
    
    return `Personalize esta mensagem de follow-up:
    
Template: "${messageTemplate}"

Contexto da conversa:
- Contato: ${context.contact?.first_name || 'Cliente'}
- Última atividade: ${context.lastMessage?.sent_at || 'Não disponível'}
- Mensagens recentes:
${recentMessages}

Gere uma versão personalizada e contextual.`;
  }
}

/**
 * Obter chave OpenAI (sempre usar master key)
 */
async function getOpenAIKey(companyId) {
  // ✅ SEMPRE usar master key (Zionic Credits)
  return CONFIG.zionicOpenAIKey || CONFIG.fallbackOpenAIKey;
}

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
