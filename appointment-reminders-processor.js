/**
 * ===============================================
 * ZIONIC APPOINTMENT REMINDERS PROCESSOR
 * ===============================================
 * Módulo especializado para processamento de lembretes de appointments
 * 
 * @author Zionic Team
 * @version 1.7.0
 */

const axios = require('axios');

// ===============================================
// UTILITÁRIOS DE LOG
// ===============================================

/**
 * Log estruturado específico para lembretes de appointments
 */
function logReminder(level, message, data = {}) {
  const timestamp = new Date().toISOString();
  const emoji = {
    info: 'ℹ️',
    success: '✅',
    warning: '⚠️',
    error: '❌',
    debug: '🔍'
  };
  
  console.log(`${emoji[level] || '📝'} [APPOINTMENT-REMINDER] [${timestamp}] ${message}`, 
    Object.keys(data).length > 0 ? JSON.stringify(data, null, 2) : '');
}

// ===============================================
// CORE: BUSCAR LEMBRETES PENDENTES
// ===============================================

/**
 * Busca lembretes de appointments prontos para execução
 */
async function getPendingAppointmentReminders(supabase, config) {
  try {
    logReminder('info', 'Buscando lembretes de appointments pendentes...');
    
    const { data: reminders, error } = await supabase.rpc('get_pending_appointment_reminders', {
      p_limit: config.maxFollowUpsPerExecution
    });
    
    if (error) {
      logReminder('error', 'Erro ao buscar lembretes pendentes', { error: error.message });
      return [];
    }
    
    const totalPending = reminders?.length || 0;
    const overdueCount = reminders?.filter(r => r.minutes_overdue > 0).length || 0;
    
    logReminder('success', `${totalPending} lembretes de appointments prontos para execução`, {
      total: totalPending,
      overdue: overdueCount,
      onTime: totalPending - overdueCount,
      method: 'sql_function'
    });
    
    return reminders || [];
    
  } catch (error) {
    logReminder('error', 'Erro ao buscar lembretes de appointments', { error: error.message });
    return [];
  }
}

/**
 * Cria lembretes automáticos para appointments futuros
 */
async function createAppointmentReminders(supabase) {
  try {
    logReminder('info', 'Criando lembretes automáticos para appointments...');
    
    // Buscar todas as empresas (coluna is_active não existe)
    const { data: companies, error: companiesError } = await supabase
      .from('companies')
      .select('id, name');
    
    if (companiesError) {
      logReminder('error', 'Erro ao buscar empresas', { error: companiesError.message });
      return 0;
    }
    
    let totalCreated = 0;
    
    for (const company of companies || []) {
      try {
        const { data: created, error } = await supabase.rpc('create_appointment_reminders', {
          p_company_id: company.id,
          p_hours_ahead: 48 // Criar lembretes para próximas 48 horas
        });
        
        if (!error && created > 0) {
          totalCreated += created;
          logReminder('debug', `${created} lembretes criados para empresa`, { 
            companyId: company.id,
            companyName: company.name,
            remindersCreated: created
          });
        }
      } catch (companyError) {
        logReminder('warning', `Erro ao criar lembretes para empresa`, { 
          companyId: company.id,
          companyName: company.name,
          error: companyError.message 
        });
      }
    }
    
    if (totalCreated > 0) {
      logReminder('success', `${totalCreated} novos lembretes de appointments criados`, {
        totalCompanies: companies?.length || 0,
        totalRemindersCreated: totalCreated
      });
    }
    
    return totalCreated;
    
  } catch (error) {
    logReminder('error', 'Erro ao criar lembretes automáticos', { error: error.message });
    return 0;
  }
}

// ===============================================
// CORE: PROCESSAR LEMBRETE
// ===============================================

/**
 * Processa um único lembrete de appointment com logs detalhados
 */
async function processAppointmentReminder(supabase, config, reminder, generatePersonalizedMessage, sendWhatsAppMessage) {
  const startTime = Date.now();
  let executionLog = {
    reminder_queue_id: reminder.id,
    appointment_id: reminder.appointment_id,
    agent_id: reminder.agent_id,
    company_id: reminder.company_id,
    rule_name: reminder.rule_name,
    success: false,
    error_message: null,
    response_time_ms: 0,
    message_sent: '',
    reminder_sent: false
  };
  
  try {
    logReminder('info', `📅 INICIANDO LEMBRETE DE APPOINTMENT`, { 
      reminderId: reminder.id,
      ruleName: reminder.rule_name,
      reminderType: reminder.reminder_type,
      contactName: reminder.contact_name || 'Nome não encontrado',
      contactPhone: reminder.contact_phone?.substring(0, 8) + '...' || 'Phone não encontrado',
      appointmentTitle: reminder.appointment_title || 'Título não encontrado',
      appointmentDate: reminder.appointment_start_time,
      appointmentLocation: reminder.appointment_location || 'Local não especificado',
      minutesBefore: reminder.minutes_before,
      minutesOverdue: reminder.minutes_overdue,
      companyId: reminder.company_id,
      agentId: reminder.agent_id,
      maxAttempts: reminder.max_attempts,
      currentAttempts: reminder.attempts
    });
    
    // ✅ Verificar se lembrete ainda está pendente
    const { data: currentStatus, error: statusError } = await supabase
      .from('appointment_reminder_queue')
      .select('status, attempts')
      .eq('id', reminder.id)
      .single();
    
    if (statusError || !currentStatus) {
      throw new Error('Lembrete não encontrado ou já foi removido');
    }
    
    if (currentStatus.status !== 'pending') {
      logReminder('warning', 'Lembrete não está mais pendente, pulando', { 
        reminderId: reminder.id,
        ruleName: reminder.rule_name,
        contactName: reminder.contact_name,
        appointmentTitle: reminder.appointment_title,
        currentStatus: currentStatus.status,
        reason: 'status_changed_before_processing'
      });
      return { success: true, skipped: true, reason: 'status_changed' };
    }
    
    // ✅ Verificar se ainda pode tentar
    if (currentStatus.attempts >= reminder.max_attempts) {
      logReminder('warning', 'Lembrete já atingiu máximo de tentativas', { 
        reminderId: reminder.id,
        ruleName: reminder.rule_name,
        contactName: reminder.contact_name,
        appointmentTitle: reminder.appointment_title,
        currentAttempts: currentStatus.attempts,
        maxAttempts: reminder.max_attempts
      });
      
      await supabase
        .from('appointment_reminder_queue')
        .update({ 
          status: 'failed',
          execution_error: `Máximo de ${reminder.max_attempts} tentativas atingido`
        })
        .eq('id', reminder.id);
        
      return { success: false, error: 'Max attempts reached' };
    }
    
    // ✅ Buscar dados do agente
    const { data: agent, error: agentError } = await supabase
      .from('ai_agents')
      .select('*')
      .eq('id', reminder.agent_id)
      .single();
      
    if (agentError || !agent) {
      throw new Error(`Agente não encontrado: ${agentError?.message}`);
    }
    
    logReminder('debug', 'Agente carregado para lembrete', {
      reminderId: reminder.id,
      ruleName: reminder.rule_name,
      agentName: agent.name,
      agentType: agent.type,
      hasAssistant: !!agent.openai_assistant_id,
      appointmentTitle: reminder.appointment_title
    });
    
    // ✅ Preparar dados do appointment para substituição de variáveis
    const appointmentDate = new Date(reminder.appointment_start_time);
    const dataFormatada = appointmentDate.toLocaleDateString('pt-BR');
    const horarioFormatado = appointmentDate.toLocaleTimeString('pt-BR', { 
      hour: '2-digit', 
      minute: '2-digit' 
    });
    
    logReminder('debug', 'Dados do appointment processados', {
      reminderId: reminder.id,
      ruleName: reminder.rule_name,
      appointmentTitle: reminder.appointment_title,
      originalDate: reminder.appointment_start_time,
      formattedDate: dataFormatada,
      formattedTime: horarioFormatado,
      location: reminder.appointment_location
    });
    
    // ✅ Preparar mensagem do lembrete com substituições básicas
    let finalMessage = reminder.message_template;
    
    // Substituir variáveis na mensagem
    if (reminder.contact_name) {
      finalMessage = finalMessage.replace(/{nome}/g, reminder.contact_name);
    }
    if (reminder.appointment_title) {
      finalMessage = finalMessage.replace(/{appointment_title}/g, reminder.appointment_title);
    }
    finalMessage = finalMessage.replace(/{data}/g, dataFormatada);
    finalMessage = finalMessage.replace(/{horario}/g, horarioFormatado);
    if (reminder.appointment_location) {
      finalMessage = finalMessage.replace(/{local}/g, reminder.appointment_location);
    }
    
    logReminder('debug', 'Variáveis substituídas na mensagem', {
      reminderId: reminder.id,
      ruleName: reminder.rule_name,
      originalTemplate: reminder.message_template.substring(0, 50) + '...',
      finalMessage: finalMessage.substring(0, 100) + '...',
      substitutions: {
        nome: reminder.contact_name,
        data: dataFormatada,
        horario: horarioFormatado,
        local: reminder.appointment_location
      }
    });
    
    // ✅ Personalizar com IA se possível
    try {
      logReminder('info', 'Personalizando lembrete com IA', {
        reminderId: reminder.id,
        ruleName: reminder.rule_name,
        contactName: reminder.contact_name,
        agentName: agent.name,
        appointmentTitle: reminder.appointment_title,
        templatePreview: finalMessage.substring(0, 50) + '...'
      });
      
      const personalizedMessage = await generatePersonalizedMessage(
        finalMessage,
        {
          appointmentId: reminder.appointment_id,
          contactName: reminder.contact_name || 'Cliente',
          contactPhone: reminder.contact_phone || '',
          appointmentTitle: reminder.appointment_title,
          appointmentDate: reminder.appointment_start_time,
          appointmentLocation: reminder.appointment_location,
          reminderType: reminder.reminder_type,
          minutesBefore: reminder.minutes_before
        },
        agent,
        reminder.company_id
      );
      
      if (personalizedMessage && personalizedMessage !== finalMessage) {
        finalMessage = personalizedMessage;
        logReminder('success', 'Lembrete personalizado com IA', {
          reminderId: reminder.id,
          ruleName: reminder.rule_name,
          contactName: reminder.contact_name,
          appointmentTitle: reminder.appointment_title,
          originalLength: reminder.message_template.length,
          finalLength: finalMessage.length,
          wasPersonalized: true
        });
      } else {
        logReminder('debug', 'IA não personalizou o lembrete, usando template com substituições', {
          reminderId: reminder.id,
          ruleName: reminder.rule_name,
          contactName: reminder.contact_name,
          appointmentTitle: reminder.appointment_title
        });
      }
    } catch (aiError) {
      logReminder('warning', 'Erro ao personalizar lembrete com IA, usando template', { 
        reminderId: reminder.id,
        ruleName: reminder.rule_name,
        contactName: reminder.contact_name,
        appointmentTitle: reminder.appointment_title,
        error: aiError.message 
      });
    }
    
    executionLog.message_sent = finalMessage;
    
    // ✅ Buscar instância WhatsApp (opcional - sistema interno como fallback)
    const { data: instance, error: instanceError } = await supabase
      .from('whatsapp_instances')
      .select('name')
      .eq('company_id', reminder.company_id)
      .eq('status', 'connected')
      .single();
    
    const instanceName = instance?.name || 'internal_system';
    
    if (instanceError || !instance?.name) {
      logReminder('debug', 'WhatsApp instance não encontrada, usando sistema interno', {
        reminderId: reminder.id,
        ruleName: reminder.rule_name,
        companyId: reminder.company_id,
        appointmentTitle: reminder.appointment_title,
        error: instanceError?.message
      });
    }
    
    logReminder('debug', 'Instância para lembrete definida', {
      reminderId: reminder.id,
      ruleName: reminder.rule_name,
      instanceName: instanceName,
      companyId: reminder.company_id,
      appointmentTitle: reminder.appointment_title,
      type: instance?.name ? 'whatsapp' : 'internal'
    });
    
    // ✅ Enviar mensagem via WhatsApp
    logReminder('info', 'Enviando lembrete de appointment via WhatsApp', {
      reminderId: reminder.id,
      ruleName: reminder.rule_name,
      reminderType: reminder.reminder_type,
      contactName: reminder.contact_name,
      contactPhone: reminder.contact_phone?.substring(0, 8) + '...',
      appointmentTitle: reminder.appointment_title,
      appointmentDate: dataFormatada,
      appointmentTime: horarioFormatado,
      instanceName: instanceName,
      messageLength: finalMessage.length,
      minutesBeforeAppointment: reminder.minutes_before
    });
    
    const sendResult = await sendWhatsAppMessage(
      instanceName,
      reminder.contact_phone,
      finalMessage
    );
    
    if (!sendResult.success) {
      throw new Error(sendResult.error);
    }
    
    // ✅ Marcar como enviado
    logReminder('debug', 'Marcando lembrete como enviado', { 
      reminderId: reminder.id,
      ruleName: reminder.rule_name,
      contactName: reminder.contact_name,
      appointmentTitle: reminder.appointment_title,
      messageId: sendResult.messageId
    });
    
    const { error: updateError } = await supabase
      .from('appointment_reminder_queue')
      .update({ 
        status: 'sent',
        attempts: reminder.attempts + 1,
        executed_at: new Date().toISOString(),
        ai_generated_message: finalMessage
      })
      .eq('id', reminder.id);
    
    if (updateError) {
      logReminder('error', 'ERRO CRÍTICO: Falha ao marcar lembrete como sent', { 
        reminderId: reminder.id,
        ruleName: reminder.rule_name,
        contactName: reminder.contact_name,
        appointmentTitle: reminder.appointment_title,
        error: updateError.message
      });
      throw new Error(`Erro ao marcar como sent: ${updateError.message}`);
    }
    
    executionLog.success = true;
    executionLog.reminder_sent = true;
    executionLog.response_time_ms = Date.now() - startTime;
    
    logReminder('success', `✅ LEMBRETE DE APPOINTMENT ENVIADO COM SUCESSO`, {
      reminderId: reminder.id,
      ruleName: reminder.rule_name,
      reminderType: reminder.reminder_type,
      contactName: reminder.contact_name,
      contactPhone: reminder.contact_phone?.substring(0, 8) + '...',
      appointmentTitle: reminder.appointment_title,
      appointmentDate: dataFormatada,
      appointmentTime: horarioFormatado,
      appointmentLocation: reminder.appointment_location,
      agentName: agent.name,
      instanceUsed: instanceName,
      responseTimeMs: executionLog.response_time_ms,
      messageLength: finalMessage.length,
      whatsappMessageId: sendResult.messageId,
      minutesBeforeAppointment: reminder.minutes_before
    });
    
    return { success: true, messageId: sendResult.messageId };
    
  } catch (error) {
    executionLog.error_message = error.message;
    executionLog.response_time_ms = Date.now() - startTime;
    
    // Atualizar tentativas
    const newAttempts = reminder.attempts + 1;
    const status = newAttempts >= reminder.max_attempts ? 'failed' : 'pending';
    
    await supabase
      .from('appointment_reminder_queue')
      .update({ 
        attempts: newAttempts,
        status: status,
        execution_error: error.message
      })
      .eq('id', reminder.id);
    
    logReminder('error', `❌ ERRO NO LEMBRETE DE APPOINTMENT`, {
      reminderId: reminder.id,
      ruleName: reminder.rule_name,
      reminderType: reminder.reminder_type,
      contactName: reminder.contact_name || 'Nome não encontrado',
      contactPhone: reminder.contact_phone?.substring(0, 8) + '...' || 'Phone não encontrado',
      appointmentTitle: reminder.appointment_title || 'Título não encontrado',
      appointmentDate: reminder.appointment_start_time,
      agentId: reminder.agent_id,
      error: error.message,
      attempts: newAttempts,
      maxAttempts: reminder.max_attempts,
      finalStatus: status,
      responseTimeMs: executionLog.response_time_ms,
      minutesBeforeAppointment: reminder.minutes_before
    });
    
    return { success: false, error: error.message };
    
  } finally {
    // Registrar log de execução
    try {
      await supabase.from('appointment_reminder_logs').insert(executionLog);
    } catch (logError) {
      logReminder('warning', 'Erro ao registrar log de lembrete', { 
        error: logError.message,
        reminderId: reminder.id,
        ruleName: reminder.rule_name,
        appointmentTitle: reminder.appointment_title
      });
    }
  }
}

// ===============================================
// EXPORTAÇÕES
// ===============================================

module.exports = {
  getPendingAppointmentReminders,
  createAppointmentReminders,
  processAppointmentReminder,
  logReminder
}; 
