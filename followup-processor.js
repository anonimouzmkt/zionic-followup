/**
 * ===============================================
 * ZIONIC FOLLOW-UP PROCESSOR
 * ===============================================
 * Módulo especializado para processamento de follow-ups de leads inativos
 * 
 * @author Zionic Team
 * @version 1.7.0
 */

const axios = require('axios');

// ===============================================
// UTILITÁRIOS DE LOG
// ===============================================

/**
 * Log estruturado específico para follow-ups
 */
function logFollowUp(level, message, data = {}) {
  const timestamp = new Date().toISOString();
  const emoji = {
    info: 'ℹ️',
    success: '✅',
    warning: '⚠️',
    error: '❌',
    debug: '🔍'
  };
  
  console.log(`${emoji[level] || '📝'} [FOLLOW-UP] [${timestamp}] ${message}`, 
    Object.keys(data).length > 0 ? JSON.stringify(data, null, 2) : '');
}

// ===============================================
// CORE: BUSCAR FOLLOW-UPS PENDENTES
// ===============================================

/**
 * Busca follow-ups prontos para execução (com filtros de agente e pausa)
 */
async function getPendingFollowUps(supabase, config) {
  try {
    logFollowUp('info', 'Buscando follow-ups pendentes...');
    
    // ✅ NOVA CONSULTA: Buscar follow-ups com verificação de agente e status
    const { data: followUps, error } = await supabase
      .from('follow_up_queue')
      .select(`
        id,
        conversation_id,
        contact_id,
        agent_id,
        company_id,
        rule_name,
        message_template,
        scheduled_at,
        attempts,
        max_attempts,
        status,
        metadata,
        contact_name,
        contact_phone,
        conversations!inner(
          ai_agent_id,
          ai_enabled,
          assigned_to,
          metadata
        )
      `)
      .eq('status', 'pending')
      .lte('scheduled_at', new Date().toISOString())
      .not('conversations.ai_agent_id', 'is', null) // ✅ Só conversas com agente atribuído
      .neq('conversations.ai_enabled', false) // ✅ Só conversas com agente ativo
      .order('scheduled_at', { ascending: true })
      .limit(config.maxFollowUpsPerExecution);
    
    if (error) {
      logFollowUp('error', 'Erro ao buscar follow-ups pendentes', { error: error.message });
      return [];
    }
    
    // ✅ FILTRO ADICIONAL: Remover conversas com follow-ups pausados
    const validFollowUps = (followUps || []).filter(followUp => {
      const conversation = followUp.conversations;
      
      // Verificar se follow-ups estão pausados na metadata
      if (conversation?.metadata?.follow_up_paused === true) {
        logFollowUp('debug', 'Follow-up filtrado - follow-ups pausados', {
          followUpId: followUp.id,
          conversationId: followUp.conversation_id,
          reason: 'follow_ups_paused_in_metadata'
        });
        return false;
      }
      
      // Verificar se conversa está atribuída a humano
      if (conversation?.assigned_to && conversation?.ai_enabled === false) {
        logFollowUp('debug', 'Follow-up filtrado - conversa atribuída a humano', {
          followUpId: followUp.id,
          conversationId: followUp.conversation_id,
          assignedTo: conversation.assigned_to,
          reason: 'assigned_to_human'
        });
        return false;
      }
      
      return true;
    });
    
    // ✅ OTIMIZAÇÃO: Calcular minutos de atraso para cada follow-up
    const enrichedFollowUps = validFollowUps.map(followUp => {
      const scheduledTime = new Date(followUp.scheduled_at).getTime();
      const currentTime = Date.now();
      const minutesOverdue = Math.max(0, Math.floor((currentTime - scheduledTime) / (1000 * 60)));
      
      return {
        ...followUp,
        minutes_overdue: minutesOverdue
      };
    });
    
    const totalPending = enrichedFollowUps.length;
    const overdueCount = enrichedFollowUps.filter(f => f.minutes_overdue > 0).length;
    const filteredOut = (followUps?.length || 0) - totalPending;
    
    logFollowUp('success', `${totalPending} follow-ups válidos prontos para execução`, {
      total: totalPending,
      overdue: overdueCount,
      onTime: totalPending - overdueCount,
      filteredOut: filteredOut,
      method: 'enhanced_filtering'
    });
    
    if (filteredOut > 0) {
      logFollowUp('info', `${filteredOut} follow-ups filtrados por regras de agente/pausa`);
    }
    
    return enrichedFollowUps;
    
  } catch (error) {
    logFollowUp('error', 'Erro ao buscar follow-ups', { error: error.message });
    return [];
  }
}

// ===============================================
// CORE: CONTEXTO DA CONVERSA
// ===============================================

/**
 * Busca contexto detalhado da conversa para follow-up
 */
async function getConversationContext(supabase, conversationId) {
  try {
    logFollowUp('debug', 'Buscando contexto da conversa', { conversationId });
    
    // Buscar dados da conversa
    const { data: conversation, error: convError } = await supabase
      .from('conversations')
      .select(`
        *,
        contact:contacts(*)
      `)
      .eq('id', conversationId)
      .single();
      
    if (convError) {
      logFollowUp('error', 'Erro ao buscar conversa', { error: convError.message, conversationId });
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
      logFollowUp('warning', 'Erro ao buscar mensagens', { error: msgError.message });
    }
    
    const context = {
      conversation,
      contact: conversation.contact,
      recentMessages: (messages || []).reverse(), // Ordem cronológica
      lastMessage: messages?.[0] || null,
      messageCount: messages?.length || 0,
      hasContactMessages: messages?.some(m => !m.sent_by_ai) || false
    };
    
    logFollowUp('debug', 'Contexto de follow-up carregado', {
      conversationId,
      contactName: context.contact?.first_name,
      contactPhone: context.contact?.phone?.substring(0, 8) + '...',
      messageCount: context.messageCount,
      hasContactMessages: context.hasContactMessages,
      lastMessageTime: context.lastMessage?.sent_at,
      companyId: conversation.company_id
    });
    
    return context;
    
  } catch (error) {
    logFollowUp('error', 'Erro ao buscar contexto de follow-up', { error: error.message, conversationId });
    return null;
  }
}

// ===============================================
// CORE: PROCESSAR FOLLOW-UP
// ===============================================

/**
 * Processa um único follow-up com logs detalhados
 */
async function processFollowUp(supabase, config, followUp, generatePersonalizedMessage, sendWhatsAppMessage) {
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
    logFollowUp('info', `🚀 INICIANDO FOLLOW-UP`, { 
      followUpId: followUp.id,
      ruleName: followUp.rule_name,
      contactName: followUp.contact_name || 'Nome não encontrado',
      contactPhone: followUp.contact_phone?.substring(0, 8) + '...' || 'Phone não encontrado',
      companyId: followUp.company_id,
      agentId: followUp.agent_id,
      minutesOverdue: followUp.minutes_overdue,
      maxAttempts: followUp.max_attempts,
      currentAttempts: followUp.attempts
    });
    
    // ✅ Verificar se follow-up ainda está pendente
    const { data: currentStatus, error: statusError } = await supabase
      .from('follow_up_queue')
      .select('status, attempts')
      .eq('id', followUp.id)
      .single();
    
    if (statusError || !currentStatus) {
      throw new Error('Follow-up não encontrado ou já foi removido');
    }
    
    if (currentStatus.status !== 'pending') {
      logFollowUp('warning', 'Follow-up não está mais pendente, pulando', { 
        followUpId: followUp.id,
        ruleName: followUp.rule_name,
        contactName: followUp.contact_name,
        currentStatus: currentStatus.status,
        reason: 'status_changed_before_processing'
      });
      return { success: true, skipped: true, reason: 'status_changed' };
    }
    
    // ✅ NOVO: Verificar se conversa tem agente atribuído e se está pausado (igual whatsapp-webhook)
    const { data: conversation, error: convError } = await supabase
      .from('conversations')
      .select('ai_agent_id, ai_enabled, metadata, assigned_to')
      .eq('id', followUp.conversation_id)
      .single();
    
    if (convError) {
      throw new Error(`Erro ao buscar dados da conversa: ${convError.message}`);
    }

    // ✅ 1. Verificar se conversa tem agente IA atribuído
    if (!conversation.ai_agent_id) {
      logFollowUp('warning', 'Follow-up cancelado - conversa não tem agente IA atribuído', { 
        followUpId: followUp.id,
        ruleName: followUp.rule_name,
        contactName: followUp.contact_name,
        conversationId: followUp.conversation_id,
        reason: 'no_agent_assigned'
      });
      
      // Marcar follow-up como cancelado
      await supabase
        .from('follow_up_queue')
        .update({ 
          status: 'cancelled',
          execution_error: 'Conversa não possui agente IA atribuído',
          metadata: {
            ...followUp.metadata,
            cancelled_reason: 'no_agent_assigned',
            cancelled_at: new Date().toISOString()
          }
        })
        .eq('id', followUp.id);
        
      return { success: true, skipped: true, reason: 'no_agent_assigned' };
    }

    // ✅ 2. Verificar se agente IA está pausado (ai_enabled = false)
    if (conversation.ai_enabled === false) {
      logFollowUp('warning', 'Follow-up cancelado - agente IA está pausado', { 
        followUpId: followUp.id,
        ruleName: followUp.rule_name,
        contactName: followUp.contact_name,
        conversationId: followUp.conversation_id,
        agentId: conversation.ai_agent_id,
        reason: 'agent_paused'
      });
      
      // Marcar follow-up como cancelado
      await supabase
        .from('follow_up_queue')
        .update({ 
          status: 'cancelled',
          execution_error: 'Agente IA está pausado para esta conversa',
          metadata: {
            ...followUp.metadata,
            cancelled_reason: 'agent_paused',
            cancelled_at: new Date().toISOString(),
            agent_id: conversation.ai_agent_id
          }
        })
        .eq('id', followUp.id);
        
      return { success: true, skipped: true, reason: 'agent_paused' };
    }

    // ✅ 3. Verificar se conversa foi atribuída a humano (igual whatsapp-webhook)
    if (conversation.assigned_to && conversation.ai_enabled === false) {
      logFollowUp('warning', 'Follow-up cancelado - conversa atribuída a agente humano', { 
        followUpId: followUp.id,
        ruleName: followUp.rule_name,
        contactName: followUp.contact_name,
        conversationId: followUp.conversation_id,
        assignedTo: conversation.assigned_to,
        reason: 'assigned_to_human'
      });
      
      // Marcar follow-up como cancelado
      await supabase
        .from('follow_up_queue')
        .update({ 
          status: 'cancelled',
          execution_error: 'Conversa atribuída a agente humano',
          metadata: {
            ...followUp.metadata,
            cancelled_reason: 'assigned_to_human',
            cancelled_at: new Date().toISOString(),
            assigned_to: conversation.assigned_to
          }
        })
        .eq('id', followUp.id);
        
      return { success: true, skipped: true, reason: 'assigned_to_human' };
    }

    // ✅ 4. Verificar se follow-ups estão pausados para esta conversa (ChatSidebar)
    if (conversation.metadata?.follow_up_paused === true) {
      logFollowUp('warning', 'Follow-up cancelado - follow-ups pausados para esta conversa', { 
        followUpId: followUp.id,
        ruleName: followUp.rule_name,
        contactName: followUp.contact_name,
        conversationId: followUp.conversation_id,
        pausedAt: conversation.metadata.follow_up_paused_at,
        pausedBy: conversation.metadata.follow_up_paused_by,
        reason: 'follow_ups_paused'
      });
      
      // Marcar follow-up como cancelado
      await supabase
        .from('follow_up_queue')
        .update({ 
          status: 'cancelled',
          execution_error: 'Follow-ups pausados manualmente para esta conversa',
          metadata: {
            ...followUp.metadata,
            cancelled_reason: 'follow_ups_paused',
            cancelled_at: new Date().toISOString(),
            paused_by_user: conversation.metadata.follow_up_paused_by,
            paused_at: conversation.metadata.follow_up_paused_at
          }
        })
        .eq('id', followUp.id);
        
      return { success: true, skipped: true, reason: 'follow_ups_paused' };
    }
    
    // ✅ Verificar se ainda pode tentar
    if (currentStatus.attempts >= followUp.max_attempts) {
      logFollowUp('warning', 'Follow-up já atingiu máximo de tentativas', { 
        followUpId: followUp.id,
        ruleName: followUp.rule_name,
        contactName: followUp.contact_name,
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
    
    // 1. Buscar contexto da conversa
    const context = await getConversationContext(supabase, followUp.conversation_id);
    if (!context) {
      throw new Error('Não foi possível carregar contexto da conversa');
    }

    // ✅ NOVA VALIDAÇÃO: Verificar status do agente antes de prosseguir (igual whatsapp-webhook)
    const agentValidation = await validateAgentConditions(supabase, context.conversation, followUp.agent_id);
    if (!agentValidation.canRespond) {
      logFollowUp('warning', 'Follow-up cancelado - agente não pode responder', {
        followUpId: followUp.id,
        ruleName: followUp.rule_name,
        contactName: followUp.contact_name,
        conversationId: followUp.conversation_id,
        agentId: followUp.agent_id,
        reason: agentValidation.reason
      });
      
      // Marcar follow-up como cancelado
      await supabase
        .from('follow_up_queue')
        .update({ 
          status: 'cancelled',
          execution_error: `Agente não pode responder: ${agentValidation.reason}`,
          metadata: {
            ...followUp.metadata,
            cancelled_reason: 'agent_validation_failed',
            cancelled_at: new Date().toISOString(),
            validation_error: agentValidation.reason
          }
        })
        .eq('id', followUp.id);
        
      return { success: true, skipped: true, reason: agentValidation.reason };
    }
    
    logFollowUp('debug', 'Contexto carregado para follow-up', {
      followUpId: followUp.id,
      ruleName: followUp.rule_name,
      contactName: context.contact?.first_name,
      lastMessageAge: context.lastMessage ? 
        Math.round((Date.now() - new Date(context.lastMessage.sent_at).getTime()) / (1000 * 60)) + ' minutos' : 
        'Sem mensagens'
    });
    
    // 2. Buscar dados do agente
    const { data: agent, error: agentError } = await supabase
      .from('ai_agents')
      .select('*')
      .eq('id', followUp.agent_id)
      .single();
      
    if (agentError || !agent) {
      throw new Error(`Agente não encontrado: ${agentError?.message}`);
    }
    
    logFollowUp('debug', 'Agente carregado para follow-up', {
      followUpId: followUp.id,
      ruleName: followUp.rule_name,
      agentName: agent.name,
      agentType: agent.type,
      hasAssistant: !!agent.openai_assistant_id
    });
    
    // 3. Gerar mensagem personalizada
    logFollowUp('info', 'Gerando mensagem personalizada para follow-up', {
      followUpId: followUp.id,
      ruleName: followUp.rule_name,
      contactName: context.contact?.first_name,
      agentName: agent.name,
      templatePreview: followUp.message_template.substring(0, 50) + '...'
    });
    
    const finalMessage = await generatePersonalizedMessage(
      followUp.message_template,
      context,
      agent,
      followUp.company_id
    );
    
    logFollowUp('success', 'Mensagem gerada para follow-up', {
      followUpId: followUp.id,
      ruleName: followUp.rule_name,
      contactName: context.contact?.first_name,
      originalLength: followUp.message_template.length,
      finalLength: finalMessage.length,
      wasPersonalized: finalMessage !== followUp.message_template
    });
    
    executionLog.message_sent = finalMessage;
    
    // 4. Buscar instância WhatsApp (opcional - sistema interno como fallback)
    const { data: instance, error: instanceError } = await supabase
      .from('whatsapp_instances')
      .select('name')
      .eq('company_id', followUp.company_id)
      .eq('status', 'connected')
      .single();
    
    const instanceName = instance?.name || 'internal_system';
    
    if (instanceError || !instance?.name) {
      logFollowUp('debug', 'WhatsApp instance não encontrada, usando sistema interno', {
        followUpId: followUp.id,
        ruleName: followUp.rule_name,
        companyId: followUp.company_id,
        error: instanceError?.message
      });
    }
    
    logFollowUp('debug', 'Instância para follow-up definida', {
      followUpId: followUp.id,
      ruleName: followUp.rule_name,
      instanceName: instanceName,
      companyId: followUp.company_id,
      type: instance?.name ? 'whatsapp' : 'internal'
    });
    
    // 5. Enviar mensagem via WhatsApp
    logFollowUp('info', 'Enviando follow-up via WhatsApp', {
      followUpId: followUp.id,
      ruleName: followUp.rule_name,
      contactName: context.contact?.first_name,
      contactPhone: context.contact?.phone?.substring(0, 8) + '...',
      instanceName: instanceName,
      messageLength: finalMessage.length
    });
    
    const sendResult = await sendWhatsAppMessage(
      instanceName,
      context.contact.phone,
      finalMessage
    );
    
    if (!sendResult.success) {
      throw new Error(sendResult.error);
    }
    
    // 6. Marcar como enviado
    logFollowUp('debug', 'Marcando follow-up como enviado', { 
      followUpId: followUp.id,
      ruleName: followUp.rule_name,
      contactName: context.contact?.first_name,
      messageId: sendResult.messageId
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
      logFollowUp('error', 'ERRO CRÍTICO: Falha ao marcar follow-up como sent', { 
        followUpId: followUp.id,
        ruleName: followUp.rule_name,
        contactName: context.contact?.first_name,
        error: updateError.message
      });
      throw new Error(`Erro ao marcar como sent: ${updateError.message}`);
    }
    
    // 7. Registrar mensagem no sistema
    const messageData = {
      conversation_id: followUp.conversation_id,
      direction: 'outbound',
      message_type: 'text',
      content: finalMessage,
      from_number: context.contact.phone,
      from_name: agent.name,
      sent_at: new Date().toISOString(),
      status: 'sent',
      sent_by_ai: true,
      external_id: null,
      metadata: {
        follow_up_id: followUp.id,
        rule_name: followUp.rule_name,
        is_follow_up: true,
        sent_via: 'follow_up_server',
        instance_name: instanceName,
        ai_agent_id: followUp.agent_id,
        agent_name: agent.name
      }
    };

    const { data: newMessage, error: messageError } = await supabase
      .from('messages')
      .insert(messageData)
      .select('id')
      .single();

    if (messageError) {
      logFollowUp('error', 'Erro ao registrar mensagem do follow-up no banco', { 
        followUpId: followUp.id,
        ruleName: followUp.rule_name,
        contactName: context.contact?.first_name,
        error: messageError.message
      });
    } else {
      logFollowUp('success', 'Mensagem de follow-up registrada no banco', { 
        followUpId: followUp.id,
        ruleName: followUp.rule_name,
        contactName: context.contact?.first_name,
        messageId: newMessage.id
      });
    }
    
    executionLog.success = true;
    executionLog.response_time_ms = Date.now() - startTime;
    
    logFollowUp('success', `✅ FOLLOW-UP ENVIADO COM SUCESSO`, {
      followUpId: followUp.id,
      ruleName: followUp.rule_name,
      contactName: context.contact?.first_name,
      contactPhone: context.contact?.phone?.substring(0, 8) + '...',
      agentName: agent.name,
      instanceUsed: instanceName,
      responseTimeMs: executionLog.response_time_ms,
      messageLength: finalMessage.length,
      whatsappMessageId: sendResult.messageId
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
    
    logFollowUp('error', `❌ ERRO NO FOLLOW-UP`, {
      followUpId: followUp.id,
      ruleName: followUp.rule_name,
      contactName: followUp.contact_name || 'Nome não encontrado',
      contactPhone: followUp.contact_phone?.substring(0, 8) + '...' || 'Phone não encontrado',
      agentId: followUp.agent_id,
      error: error.message,
      attempts: newAttempts,
      maxAttempts: followUp.max_attempts,
      finalStatus: status,
      responseTimeMs: executionLog.response_time_ms
    });
    
    return { success: false, error: error.message };
    
  } finally {
    // Registrar log de execução
    try {
      await supabase.from('follow_up_logs').insert(executionLog);
    } catch (logError) {
      logFollowUp('warning', 'Erro ao registrar log de follow-up', { 
        error: logError.message,
        followUpId: followUp.id,
        ruleName: followUp.rule_name
      });
    }
  }
}

// ===============================================
// VALIDAÇÃO DE AGENTE (copiada do whatsapp-webhook)
// ===============================================

/**
 * Valida se agente pode responder baseado nas condições (igual whatsapp-webhook)
 */
async function validateAgentConditions(supabase, conversation, agentId) {
  try {
    // 1. Buscar dados do agente
    const { data: agent, error: agentError } = await supabase
      .from('ai_agents')
      .select('status, handoff_triggers')
      .eq('id', agentId)
      .single();

    if (agentError || !agent) {
      return {
        canRespond: false,
        reason: 'Agent not found'
      };
    }

    // 2. Verificar se agente está ativo
    if (agent.status !== 'active' && agent.status !== 'error') {
      return {
        canRespond: false,
        reason: 'Agent is not active'
      };
    }

    // 3. Verificar se conversa foi atribuída a humano
    if (conversation.assigned_to && conversation.ai_enabled === false) {
      return {
        canRespond: false,
        reason: 'Conversation assigned to human agent'
      };
    }

    // 4. Verificar se agente IA foi pausado manualmente
    if (conversation.ai_enabled === false) {
      return {
        canRespond: false,
        reason: 'AI agent manually paused by user'
      };
    }

    // 5. Verificar se follow-ups estão pausados
    if (conversation.metadata?.follow_up_paused === true) {
      return {
        canRespond: false,
        reason: 'Follow-ups paused for this conversation'
      };
    }

    return {
      canRespond: true
    };
  } catch (error) {
    logFollowUp('error', 'Erro ao validar condições do agente', { 
      error: error.message,
      agentId,
      conversationId: conversation.id 
    });
    return {
      canRespond: false,
      reason: 'Validation error'
    };
  }
}

// ===============================================
// EXPORTAÇÕES
// ===============================================

module.exports = {
  getPendingFollowUps,
  processFollowUp,
  logFollowUp,
  validateAgentConditions
}; 
