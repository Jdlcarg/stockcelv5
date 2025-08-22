import { useState, useEffect } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { Link, useLocation } from 'wouter';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { KeyRound, ArrowLeft, CheckCircle, AlertCircle, Eye, EyeOff } from 'lucide-react';
import { apiRequest } from '@/lib/queryClient';

export default function ResetPasswordPage() {
  const [, setLocation] = useLocation();
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [resetComplete, setResetComplete] = useState(false);

  // Obtener token de la URL
  const urlParams = new URLSearchParams(window.location.search);
  const token = urlParams.get('token');

  // Validar token al cargar la página
  const tokenValidation = useQuery({
    queryKey: ['validate-reset-token', token],
    queryFn: () => apiRequest('GET', `/api/auth/validate-reset-token/${token}`),
    enabled: !!token,
    retry: false,
  });

  const resetPasswordMutation = useMutation({
    mutationFn: (data: { token: string; newPassword: string; confirmPassword: string }) => 
      apiRequest('POST', '/api/auth/reset-password', data),
    onSuccess: () => {
      setResetComplete(true);
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!token) return;
    
    resetPasswordMutation.mutate({
      token,
      newPassword,
      confirmPassword,
    });
  };

  // Redirigir si no hay token
  useEffect(() => {
    if (!token) {
      setLocation('/forgot-password');
    }
  }, [token, setLocation]);

  if (!token) {
    return null;
  }

  // Pantalla de éxito
  if (resetComplete) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-green-50 to-emerald-100 dark:from-gray-900 dark:to-gray-800 p-4">
        <Card className="w-full max-w-md shadow-xl">
          <CardHeader className="text-center">
            <div className="w-16 h-16 bg-green-100 dark:bg-green-900 rounded-full flex items-center justify-center mx-auto mb-4">
              <CheckCircle className="w-8 h-8 text-green-600 dark:text-green-400" />
            </div>
            <CardTitle className="text-2xl font-bold text-gray-900 dark:text-white">
              ¡Contraseña Actualizada!
            </CardTitle>
            <CardDescription className="text-gray-600 dark:text-gray-400">
              Tu contraseña se ha cambiado exitosamente
            </CardDescription>
          </CardHeader>
          
          <CardContent>
            <Alert>
              <CheckCircle className="h-4 w-4" />
              <AlertDescription>
                Ya puedes iniciar sesión con tu nueva contraseña
              </AlertDescription>
            </Alert>
          </CardContent>
          
          <CardFooter>
            <Link href="/" className="w-full">
              <Button className="w-full">
                Ir al Login
              </Button>
            </Link>
          </CardFooter>
        </Card>
      </div>
    );
  }

  // Pantalla de carga y validación de token
  if (tokenValidation.isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-blue-50 to-indigo-100 dark:from-gray-900 dark:to-gray-800 p-4">
        <Card className="w-full max-w-md shadow-xl">
          <CardContent className="flex flex-col items-center p-8">
            <div className="w-8 h-8 border-3 border-blue-600 border-t-transparent rounded-full animate-spin mb-4" />
            <p className="text-gray-600 dark:text-gray-400">Validando enlace de recuperación...</p>
          </CardContent>
        </Card>
      </div>
    );
  }

  // Token inválido o expirado
  if (tokenValidation.error) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-red-50 to-rose-100 dark:from-gray-900 dark:to-gray-800 p-4">
        <Card className="w-full max-w-md shadow-xl">
          <CardHeader className="text-center">
            <div className="w-16 h-16 bg-red-100 dark:bg-red-900 rounded-full flex items-center justify-center mx-auto mb-4">
              <AlertCircle className="w-8 h-8 text-red-600 dark:text-red-400" />
            </div>
            <CardTitle className="text-2xl font-bold text-gray-900 dark:text-white">
              Enlace Inválido
            </CardTitle>
            <CardDescription className="text-gray-600 dark:text-gray-400">
              Este enlace de recuperación no es válido
            </CardDescription>
          </CardHeader>
          
          <CardContent>
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>
                {(tokenValidation.error as any)?.message || 'El enlace ha expirado o ya fue utilizado'}
              </AlertDescription>
            </Alert>
          </CardContent>
          
          <CardFooter className="flex flex-col gap-3">
            <Link href="/forgot-password" className="w-full">
              <Button className="w-full">
                Solicitar Nuevo Enlace
              </Button>
            </Link>
            
            <Link href="/">
              <Button variant="ghost" className="w-full">
                <ArrowLeft className="w-4 h-4 mr-2" />
                Volver al Login
              </Button>
            </Link>
          </CardFooter>
        </Card>
      </div>
    );
  }

  const userData = tokenValidation.data;

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-blue-50 to-indigo-100 dark:from-gray-900 dark:to-gray-800 p-4">
      <Card className="w-full max-w-md shadow-xl">
        <CardHeader className="text-center">
          <div className="w-16 h-16 bg-blue-100 dark:bg-blue-900 rounded-full flex items-center justify-center mx-auto mb-4">
            <KeyRound className="w-8 h-8 text-blue-600 dark:text-blue-400" />
          </div>
          <CardTitle className="text-2xl font-bold text-gray-900 dark:text-white">
            Nueva Contraseña
          </CardTitle>
          <CardDescription className="text-gray-600 dark:text-gray-400">
            Configura una nueva contraseña para tu cuenta
          </CardDescription>
        </CardHeader>
        
        <form onSubmit={handleSubmit}>
          <CardContent className="space-y-4">
            {resetPasswordMutation.error && (
              <Alert variant="destructive">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription>
                  {(resetPasswordMutation.error as any)?.message || 'Error actualizando contraseña'}
                </AlertDescription>
              </Alert>
            )}
            
            <div className="space-y-2">
              <label htmlFor="newPassword" className="text-sm font-medium text-gray-700 dark:text-gray-300">
                Nueva Contraseña
              </label>
              <div className="relative">
                <Input
                  id="newPassword"
                  type={showPassword ? "text" : "password"}
                  placeholder="Mínimo 6 caracteres"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  required
                  minLength={6}
                  className="w-full pr-10"
                  disabled={resetPasswordMutation.isPending}
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  className="absolute right-0 top-0 h-full px-3 hover:bg-transparent"
                  onClick={() => setShowPassword(!showPassword)}
                >
                  {showPassword ? (
                    <EyeOff className="h-4 w-4 text-gray-400" />
                  ) : (
                    <Eye className="h-4 w-4 text-gray-400" />
                  )}
                </Button>
              </div>
            </div>
            
            <div className="space-y-2">
              <label htmlFor="confirmPassword" className="text-sm font-medium text-gray-700 dark:text-gray-300">
                Confirmar Contraseña
              </label>
              <div className="relative">
                <Input
                  id="confirmPassword"
                  type={showConfirmPassword ? "text" : "password"}
                  placeholder="Repite la contraseña"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  required
                  minLength={6}
                  className="w-full pr-10"
                  disabled={resetPasswordMutation.isPending}
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  className="absolute right-0 top-0 h-full px-3 hover:bg-transparent"
                  onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                >
                  {showConfirmPassword ? (
                    <EyeOff className="h-4 w-4 text-gray-400" />
                  ) : (
                    <Eye className="h-4 w-4 text-gray-400" />
                  )}
                </Button>
              </div>
            </div>
            
            {newPassword && confirmPassword && newPassword !== confirmPassword && (
              <Alert variant="destructive">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription>
                  Las contraseñas no coinciden
                </AlertDescription>
              </Alert>
            )}
            
            <div className="bg-blue-50 dark:bg-blue-900/20 p-3 rounded-lg">
              <h4 className="font-medium text-blue-900 dark:text-blue-100 mb-1">
                Requisitos de la contraseña:
              </h4>
              <ul className="text-sm text-blue-800 dark:text-blue-200 space-y-1">
                <li className={`flex items-center gap-2 ${newPassword.length >= 6 ? 'text-green-600 dark:text-green-400' : ''}`}>
                  {newPassword.length >= 6 ? '✓' : '•'} Mínimo 6 caracteres
                </li>
                <li className={`flex items-center gap-2 ${newPassword && confirmPassword && newPassword === confirmPassword ? 'text-green-600 dark:text-green-400' : ''}`}>
                  {newPassword && confirmPassword && newPassword === confirmPassword ? '✓' : '•'} Ambas contraseñas deben coincidir
                </li>
              </ul>
            </div>
          </CardContent>
          
          <CardFooter className="flex flex-col gap-3">
            <Button 
              type="submit" 
              className="w-full"
              disabled={
                !newPassword || 
                !confirmPassword || 
                newPassword !== confirmPassword || 
                newPassword.length < 6 ||
                resetPasswordMutation.isPending
              }
            >
              {resetPasswordMutation.isPending ? (
                <div className="flex items-center gap-2">
                  <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                  Actualizando...
                </div>
              ) : (
                <>
                  <KeyRound className="w-4 h-4 mr-2" />
                  Actualizar Contraseña
                </>
              )}
            </Button>
            
            <Link href="/">
              <Button variant="ghost" className="w-full">
                <ArrowLeft className="w-4 h-4 mr-2" />
                Cancelar
              </Button>
            </Link>
          </CardFooter>
        </form>
      </Card>
    </div>
  );
}