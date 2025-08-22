import { Router } from 'express';
import crypto from 'crypto';
import bcryptjs from 'bcryptjs';
import { storage } from './storage';
import { sendEmail, generatePasswordResetEmail } from './email';
import { passwordResetTokens } from '@shared/schema';
import { eq, and, gt } from 'drizzle-orm';

const router = Router();

// Solicitar reset de contraseña
router.post('/forgot-password', async (req, res) => {
  try {
    const { email } = req.body;

    if (!email) {
      return res.status(400).json({ message: 'Email es requerido' });
    }

    console.log('🔍 Buscando usuario con email:', email);

    // Buscar usuario por email
    const user = await storage.getUserByEmail(email);
    if (!user) {
      // Por seguridad, siempre responder exitosamente aunque el email no exista
      return res.json({ 
        message: 'Si el email existe en nuestro sistema, recibirás un enlace de recuperación' 
      });
    }

    console.log('✅ Usuario encontrado:', user.username);

    // Generar token único
    const token = crypto.randomBytes(32).toString('hex');
    const expiresAt = new Date(Date.now() + 60 * 60 * 1000); // 1 hora

    // Guardar token en base de datos
    await storage.createPasswordResetToken({
      userId: user.id,
      token,
      expiresAt,
      used: false,
    });

    console.log('🔐 Token generado:', token.substring(0, 8) + '...');

    // Generar URL de reset
    const baseUrl = req.get('host')?.includes('localhost') 
      ? `http://${req.get('host')}`
      : `https://${req.get('host')}`;
    const resetUrl = `${baseUrl}/reset-password?token=${token}`;

    console.log('🔗 URL de reset:', resetUrl);

    // Generar contenido del email
    const emailContent = generatePasswordResetEmail(resetUrl, user.email);

    // Enviar email
    const emailSent = await sendEmail({
      to: user.email,
      subject: emailContent.subject,
      html: emailContent.html,
      text: emailContent.text,
    });

    if (emailSent) {
      console.log('📧 Email enviado exitosamente a:', user.email);
      res.json({ 
        message: 'Email de recuperación enviado. Revisa tu bandeja de entrada.' 
      });
    } else {
      console.error('❌ Error enviando email a:', user.email);
      res.status(500).json({ 
        message: 'Error enviando email. Intenta nuevamente o contacta al administrador.' 
      });
    }

  } catch (error) {
    console.error('❌ Error en forgot-password:', error);
    res.status(500).json({ 
      message: 'Error interno del servidor' 
    });
  }
});

// Validar token de reset
router.get('/validate-reset-token/:token', async (req, res) => {
  try {
    const { token } = req.params;

    console.log('🔍 Validando token:', token.substring(0, 8) + '...');

    const resetToken = await storage.getPasswordResetToken(token);
    if (!resetToken) {
      return res.status(400).json({ 
        message: 'Token inválido o expirado' 
      });
    }

    if (resetToken.used) {
      return res.status(400).json({ 
        message: 'Este enlace ya fue utilizado' 
      });
    }

    if (new Date() > resetToken.expiresAt) {
      return res.status(400).json({ 
        message: 'Este enlace ha expirado. Solicita uno nuevo.' 
      });
    }

    const user = await storage.getUserById(resetToken.userId);
    if (!user) {
      return res.status(400).json({ 
        message: 'Usuario no encontrado' 
      });
    }

    console.log('✅ Token válido para usuario:', user.username);

    res.json({ 
      valid: true,
      email: user.email,
      username: user.username
    });

  } catch (error) {
    console.error('❌ Error validando token:', error);
    res.status(500).json({ 
      message: 'Error interno del servidor' 
    });
  }
});

// Reset de contraseña
router.post('/reset-password', async (req, res) => {
  try {
    const { token, newPassword, confirmPassword } = req.body;

    if (!token || !newPassword || !confirmPassword) {
      return res.status(400).json({ 
        message: 'Todos los campos son requeridos' 
      });
    }

    if (newPassword !== confirmPassword) {
      return res.status(400).json({ 
        message: 'Las contraseñas no coinciden' 
      });
    }

    if (newPassword.length < 6) {
      return res.status(400).json({ 
        message: 'La contraseña debe tener al menos 6 caracteres' 
      });
    }

    console.log('🔍 Procesando reset con token:', token.substring(0, 8) + '...');

    const resetToken = await storage.getPasswordResetToken(token);
    if (!resetToken) {
      return res.status(400).json({ 
        message: 'Token inválido o expirado' 
      });
    }

    if (resetToken.used) {
      return res.status(400).json({ 
        message: 'Este enlace ya fue utilizado' 
      });
    }

    if (new Date() > resetToken.expiresAt) {
      return res.status(400).json({ 
        message: 'Este enlace ha expirado. Solicita uno nuevo.' 
      });
    }

    const user = await storage.getUserById(resetToken.userId);
    if (!user) {
      return res.status(400).json({ 
        message: 'Usuario no encontrado' 
      });
    }

    console.log('🔐 Actualizando contraseña para usuario:', user.username);

    // Actualizar contraseña
    await storage.updateUser(user.id, { 
      password: newPassword,
      mustChangePassword: false 
    });

    // Marcar token como usado
    await storage.markPasswordResetTokenAsUsed(resetToken.id);

    console.log('✅ Contraseña actualizada exitosamente para:', user.username);

    res.json({ 
      message: 'Contraseña actualizada exitosamente. Ya puedes iniciar sesión.' 
    });

  } catch (error) {
    console.error('❌ Error en reset-password:', error);
    res.status(500).json({ 
      message: 'Error interno del servidor' 
    });
  }
});

export default router;