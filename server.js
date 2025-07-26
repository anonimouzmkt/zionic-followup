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
    log('debug', 'Verificando configuração das chaves OpenAI', {
      hasMasterKey: !!CONFIG.masterOpenAIKey,
      hasFallbackKey: !!CONFIG.fallbackOpenAIKey,
      masterKeyPrefix: CONFIG.masterOpenAIKey ? CONFIG.masterOpenAIKey.substring(0, 7) + '...' : 'null',
      fallbackKeyPrefix: CONFIG.fallbackOpenAIKey ? CONFIG.fallbackOpenAIKey.substring(0, 7) + '...' : 'null'
    });

    let openaiApiKey = CONFIG.masterOpenAIKey;
    
    if (!openaiApiKey) {
      log('warning', 'Master key não encontrada, usando fallback');
      openaiApiKey = CONFIG.fallbackOpenAIKey;
    }
    
    if (!openaiApiKey) {
      log('error', 'Nenhuma chave OpenAI disponível');
      return messageTemplate;
    }

    log('debug', 'Chave OpenAI selecionada', {
      keyPrefix: openaiApiKey.substring(0, 7) + '...',
      keyLength: openaiApiKey.length,
      isValidFormat: openaiApiKey.startsWith('sk-')
    });

    // ✅ 2. Para follow-ups, SEMPRE usar thread existente da conversa
    if (context.conversation && !context.appointmentId) {
      const threadId = context.conversation.openai_thread_id;
      
      log('debug', 'Verificando thread da conversa', {
        hasThread: !!threadId,
        threadId: threadId,
        conversationId: context.conversation.id,
        threadFormat: threadId ? (threadId.startsWith('thread_') ? 'valid' : 'invalid') : 'none'
      });
      
      if (!threadId) {
        log('warning', 'Conversa sem thread OpenAI - usando template simples para preservar contexto');
        return messageTemplate;
      }
      
      if (!threadId.startsWith('thread_')) {
        log('error', 'Thread ID em formato inválido', { 
          threadId: threadId,
          conversationId: context.conversation.id
        });
        return messageTemplate;
      }
      
      log('debug', 'Reutilizando thread existente para follow-up', {
        threadId: threadId,
        conversationId: context.conversation.id
      });
      
      // ✅ 3. Verificar se thread ainda existe no OpenAI
      try {
        log('debug', 'Verificando se thread existe no OpenAI', { threadId });
        
        const threadCheckResponse = await axios.get(`https://api.openai.com/v1/threads/${threadId}`, {
          headers: {
            'Authorization': `Bearer ${openaiApiKey}`,
            'OpenAI-Beta': 'assistants=v2'
          }
        });

        log('success', 'Thread verificada e existe no OpenAI', { 
          threadId,
          status: threadCheckResponse.status 
        });
      } catch (threadCheckError) {
        log('error', 'Thread não existe ou é inválida no OpenAI', {
          threadId,
          error: threadCheckError.message,
          status: threadCheckError.response?.status,
          data: threadCheckError.response?.data
        });
        return messageTemplate;
      }

      // ✅ 4. Tentar gerar resposta personalizada usando thread existente
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
    
         // ✅ 5. Para appointments ou outros casos, usar geração simples
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

  // ✅ Adicionar mensagem à thread existente usando função auxiliar (igual ao automation-ai-handler)  
  await addMessageToOpenAIThread(openaiApiKey, threadId, contextualPrompt, 'user');

  // ✅ Se agente tem assistant, usar Assistant API
  if (agent.openai_assistant_id && agent.openai_assistant_id.startsWith('asst_')) {
    return await executeAssistantRun(threadId, agent, openaiApiKey);
  } else {
    // ✅ Usar completion direta com contexto da thread
    return await executeDirectCompletion(contextualPrompt, agent, openaiApiKey);
  }
}

/**
 * ✅ NOVO: Executar Assistant API (copiado EXATAMENTE do automation-ai-handler)
 */
async function executeAssistantRun(threadId, agent, openaiApiKey) {
  try {
    log('debug', 'Iniciando execução do Assistant', {
      threadId: threadId,
      assistantId: agent.openai_assistant_id,
      agentId: agent.id,
      hasApiKey: !!openaiApiKey
    });

    // ✅ CORRIGIDO: Usar mesmo payload do automation-ai-handler (SEM max_tokens para Assistant API)
    let runPayload;
    
    if (agent.openai_assistant_id) {
      runPayload = {
        assistant_id: agent.openai_assistant_id
      };
    } else if (agent.assistant_id) {
      runPayload = {
        assistant_id: agent.assistant_id
      };
    } else {
      runPayload = {
        model: agent.openai_model || 'gpt-4o-mini',
        temperature: agent.temperature ?? 0.7,
        max_tokens: agent.max_tokens || 300,
        instructions: agent.system_prompt || `Você é ${agent.name}, um assistente de atendimento ao cliente.`
      };
    }

    const runResponse = await axios.post(`https://api.openai.com/v1/threads/${threadId}/runs`, runPayload, {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${openaiApiKey}`,
        'OpenAI-Beta': 'assistants=v2'
      }
    });

    if (!runResponse.data || !runResponse.data.id) {
      throw new Error('Failed to start run - no run ID returned');
    }

    const runData = runResponse.data;
    const runId = runData.id;
    let status = runData.status;

    log('success', `Run started: ${runId}, status: ${status}`);

    // Poll for completion (igual ao automation-ai-handler)
    let attempts = 0;
    const maxAttempts = 30;

    while (status !== 'completed' && status !== 'failed' && attempts < maxAttempts) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      attempts++;

      const statusResponse = await axios.get(`https://api.openai.com/v1/threads/${threadId}/runs/${runId}`, {
        headers: {
          'Authorization': `Bearer ${openaiApiKey}`,
          'OpenAI-Beta': 'assistants=v2'
        }
      });

      if (statusResponse.data) {
        status = statusResponse.data.status;
        log('debug', `Run status: ${status} (attempt ${attempts})`);
        
        if (status === 'failed') {
          log('error', 'Run failed:', statusResponse.data);
          break;
        }
      }
    }

    if (status === 'completed') {
      log('success', 'OpenAI run completed successfully');
      
      // ✅ Buscar dados finais do run para pegar usage (igual ao automation-ai-handler)
      log('debug', `Fetching final run data for usage: ${runId}`);
      
      const finalResponse = await axios.get(`https://api.openai.com/v1/threads/${threadId}/runs/${runId}`, {
        headers: {
          'Authorization': `Bearer ${openaiApiKey}`,
          'OpenAI-Beta': 'assistants=v2'
        }
      });

      if (finalResponse.data && finalResponse.data.usage) {
        log('debug', 'Assistant run usage:', finalResponse.data.usage);
        log('success', `Usage found: ${finalResponse.data.usage.total_tokens} tokens`);
      } else {
        log('warning', 'Usage field is null/undefined in response');
      }
      
      // Buscar última mensagem do assistant
      const messagesResponse = await axios.get(`https://api.openai.com/v1/threads/${threadId}/messages?order=desc&limit=1`, {
        headers: {
          'Authorization': `Bearer ${openaiApiKey}`,
          'OpenAI-Beta': 'assistants=v2'
        }
      });

      const messages = messagesResponse.data.data;
      for (const message of messages) {
        if (message.role === 'assistant' && message.content?.length > 0) {
          const textContent = message.content.find(c => c.type === 'text');
          if (textContent?.text?.value) {
            return textContent.text.value.trim();
          }
        }
      }

      throw new Error('No response from OpenAI assistant');
      
    } else if (status === 'failed') {
      throw new Error('OpenAI run failed');
    } else {
      throw new Error('OpenAI run timeout');
    }

  } catch (error) {
    log('error', 'ERRO DETALHADO ao criar Assistant Run', {
      error: error.message,
      status: error.response?.status,
      statusText: error.response?.statusText,
      data: error.response?.data,
      threadId: threadId,
      assistantId: agent.openai_assistant_id || agent.assistant_id,
      hasApiKey: !!openaiApiKey
    });
    throw error;
  }
}

/**
 * ✅ NOVO: Geração direta via Completion API
 */
async function executeDirectCompletion(prompt, agent, openaiApiKey) {
  try {
    log('debug', 'Iniciando completion direta', {
      model: agent.openai_model || 'gpt-4',
      agentId: agent.id,
      agentName: agent.name,
      hasApiKey: !!openaiApiKey,
      promptLength: prompt.length
    });

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

    log('success', 'Completion executada com sucesso', {
      responseLength: response.length,
      model: agent.openai_model || 'gpt-4'
    });

    return response;
  } catch (completionError) {
    log('error', 'ERRO DETALHADO na completion direta', {
      error: completionError.message,
      status: completionError.response?.status,
      statusText: completionError.response?.statusText,
      data: completionError.response?.data,
      model: agent.openai_model || 'gpt-4',
      agentId: agent.id,
      hasApiKey: !!openaiApiKey
    });
    throw completionError;
  }
}

/**
 * ✅ NOVO: Geração para appointments ou casos sem thread
 */
async function generateWithDirectAPI(messageTemplate, context, agent, openaiApiKey) {
  try {
    log('debug', 'Iniciando geração direta (appointments ou sem thread)', {
      hasAppointment: !!context.appointmentId,
      hasContact: !!context.contact,
      agentId: agent.id,
      templateLength: messageTemplate.length
    });

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

    log('debug', 'Prompt construído para geração direta', {
      promptLength: prompt.length,
      isAppointment: !!context.appointmentId
    });

    return await executeDirectCompletion(prompt, agent, openaiApiKey);
  } catch (directError) {
    log('error', 'ERRO na geração direta (appointments ou sem thread)', {
      error: directError.message,
      hasAppointment: !!context.appointmentId,
      agentId: agent.id
    });
    throw directError;
  }
}

// ✅ FUNÇÃO AUXILIAR: Obter ou criar thread OpenAI (copiada EXATAMENTE do automation-ai-handler)
async function getOrCreateOpenAIThread(
  apiKey,
  conversationId,
  agent,
  context
) {
  try {
    // Verificar se já existe thread
    const { data: conversation } = await supabase
      .from('conversations')
      .select('openai_thread_id')
      .eq('id', conversationId)
      .single();

    if (conversation?.openai_thread_id) {
      log('debug', `🔄 Reusing existing thread: ${conversation.openai_thread_id}`);
      return conversation.openai_thread_id;
    }

    // Criar novo thread
    log('debug', '🆕 Creating new OpenAI thread...');
    const response = await fetch('https://api.openai.com/v1/threads', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'OpenAI-Beta': 'assistants=v2'
      }
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => null);
      throw new Error(`Failed to create thread: ${response.status} - ${errorData?.error?.message || 'Unknown error'}`);
    }

    const threadData = await response.json();
    const threadId = threadData.id;

    // Salvar thread ID
    await supabase
      .from('conversations')
      .update({ 
        openai_thread_id: threadId,
        updated_at: new Date().toISOString()
      })
      .eq('id', conversationId);

    log('success', `✅ Created thread: ${threadId}`);
    return threadId;

  } catch (error) {
    log('error', '❌ Error creating thread:', error);
    return null;
  }
}

// ✅ FUNÇÃO AUXILIAR: Adicionar mensagem ao thread OpenAI (copiada EXATAMENTE do automation-ai-handler)
async function addMessageToOpenAIThread(
  apiKey,
  threadId,
  content,
  role
) {
  try {
    const response = await fetch(`https://api.openai.com/v1/threads/${threadId}/messages`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'OpenAI-Beta': 'assistants=v2'
      },
      body: JSON.stringify({
        role: role,
        content: content
      })
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => null);
      throw new Error(`Failed to add message: ${response.status} - ${errorData?.error?.message || 'Unknown error'}`);
    }

    log('debug', `📨 ${role} message added to thread`);
  } catch (error) {
    log('error', '❌ Error adding message:', error);
    throw error;
  }
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
