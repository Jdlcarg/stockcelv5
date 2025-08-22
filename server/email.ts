import nodemailer from 'nodemailer';

if (!process.env.GMAIL_USER || !process.env.GMAIL_PASS) {
  throw new Error("GMAIL_USER and GMAIL_PASS environment variables must be set");
}

// Configuración del transporter de Gmail
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.GMAIL_USER,
    pass: process.env.GMAIL_PASS,
  },
});

interface EmailParams {
  to: string;
  subject: string;
  text?: string;
  html?: string;
}

export async function sendEmail(params: EmailParams): Promise<boolean> {
  try {
    const mailOptions = {
      from: `"StockCel System" <${process.env.GMAIL_USER}>`,
      to: params.to,
      subject: params.subject,
      text: params.text,
      html: params.html,
    };

    const result = await transporter.sendMail(mailOptions);
    console.log('📧 Email enviado exitosamente:', result.messageId);
    return true;
  } catch (error) {
    console.error('❌ Error enviando email:', error);
    return false;
  }
}

export function generatePasswordResetEmail(resetUrl: string, userEmail: string): { subject: string; html: string; text: string } {
  const subject = 'StockCel - Recuperación de Contraseña';
  
  const html = `
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8">
      <title>${subject}</title>
      <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
        .container { max-width: 600px; margin: 0 auto; padding: 20px; }
        .header { background: linear-gradient(135deg, #3b82f6, #1e40af); color: white; padding: 30px; text-align: center; border-radius: 8px 8px 0 0; }
        .content { background: #f8fafc; padding: 30px; border-radius: 0 0 8px 8px; }
        .button { display: inline-block; background: #3b82f6; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: bold; margin: 20px 0; }
        .button:hover { background: #1e40af; }
        .footer { margin-top: 30px; padding-top: 20px; border-top: 1px solid #e2e8f0; font-size: 14px; color: #64748b; }
        .warning { background: #fef3c7; border: 1px solid #f59e0b; padding: 15px; border-radius: 6px; margin: 20px 0; }
      </style>
    </head>
    <body>
      <div class="container">
        <div class="header">
          <h1>🔐 StockCel</h1>
          <p>Recuperación de Contraseña</p>
        </div>
        <div class="content">
          <h2>Hola,</h2>
          <p>Recibimos una solicitud para restablecer la contraseña de tu cuenta en <strong>StockCel</strong>.</p>
          <p>Email de la cuenta: <strong>${userEmail}</strong></p>
          
          <p>Para crear una nueva contraseña, haz clic en el siguiente enlace:</p>
          
          <div style="text-align: center;">
            <a href="${resetUrl}" class="button">Restablecer Contraseña</a>
          </div>
          
          <div class="warning">
            <strong>⚠️ Importante:</strong>
            <ul>
              <li>Este enlace expira en <strong>1 hora</strong></li>
              <li>Solo puedes usarlo una vez</li>
              <li>Si no solicitaste este cambio, ignora este email</li>
            </ul>
          </div>
          
          <p>Si el botón no funciona, copia y pega este enlace en tu navegador:</p>
          <p style="word-break: break-all; background: #e2e8f0; padding: 10px; border-radius: 4px; font-family: monospace;">
            ${resetUrl}
          </p>
          
          <div class="footer">
            <p><strong>StockCel - Sistema de Gestión de Stock y Ventas</strong></p>
            <p>Este es un email automático, no respondas a este mensaje.</p>
            <p>Si tienes problemas, contacta al administrador del sistema.</p>
          </div>
        </div>
      </div>
    </body>
    </html>
  `;
  
  const text = `
StockCel - Recuperación de Contraseña

Hola,

Recibimos una solicitud para restablecer la contraseña de tu cuenta en StockCel.

Email de la cuenta: ${userEmail}

Para crear una nueva contraseña, visita el siguiente enlace:
${resetUrl}

IMPORTANTE:
- Este enlace expira en 1 hora
- Solo puedes usarlo una vez
- Si no solicitaste este cambio, ignora este email

Si tienes problemas, contacta al administrador del sistema.

StockCel - Sistema de Gestión de Stock y Ventas
  `;
  
  return { subject, html, text };
}