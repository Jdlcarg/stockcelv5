import { useState, useEffect } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Crown, Calendar, Clock, AlertTriangle, Phone, Mail, MessageCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";

interface SubscriptionStatusProps {
  client: {
    id: number;
    name: string;
    subscriptionType: string;
    trialStartDate?: string;
    trialEndDate?: string;
    salesContactNumber?: string;
  };
  user: {
    role: string;
  };
}

export function SubscriptionStatus({ client, user }: SubscriptionStatusProps) {
  const [timeRemaining, setTimeRemaining] = useState<{
    days: number;
    hours: number;
    minutes: number;
    seconds: number;
    isExpired: boolean;
  }>({ days: 0, hours: 0, minutes: 0, seconds: 0, isExpired: false });
  
  const [currentClient, setCurrentClient] = useState(client);

  // Escuchar eventos de actualización de cliente
  useEffect(() => {
    const handleClientUpdate = (event: CustomEvent) => {
      if (event.detail.clientId === client.id) {
        // Actualizar el cliente local con los nuevos datos
        setCurrentClient(prev => ({
          ...prev,
          ...event.detail.updatedData
        }));
        
        // Recargar la página para aplicar cambios inmediatamente
        window.location.reload();
      }
    };

    window.addEventListener('clientUpdated', handleClientUpdate as EventListener);
    
    return () => {
      window.removeEventListener('clientUpdated', handleClientUpdate as EventListener);
    };
  }, [client.id]);

  // Calcular tiempo restante
  useEffect(() => {
    if (currentClient.subscriptionType !== "trial" || !currentClient.trialEndDate) return;

    const calculateTimeRemaining = () => {
      const now = new Date();
      const endDate = new Date(currentClient.trialEndDate || now);
      const difference = endDate.getTime() - now.getTime();

      if (difference <= 0) {
        setTimeRemaining({ days: 0, hours: 0, minutes: 0, seconds: 0, isExpired: true });
        return;
      }

      const days = Math.floor(difference / (1000 * 60 * 60 * 24));
      const hours = Math.floor((difference % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
      const minutes = Math.floor((difference % (1000 * 60 * 60)) / (1000 * 60));
      const seconds = Math.floor((difference % (1000 * 60)) / 1000);

      setTimeRemaining({ days, hours, minutes, seconds, isExpired: false });
    };

    calculateTimeRemaining();
    const interval = setInterval(calculateTimeRemaining, 1000);

    return () => clearInterval(interval);
  }, [currentClient.trialEndDate]);

  // Determinar el estado de la suscripción
  const getSubscriptionStatus = () => {
    if (currentClient.subscriptionType === "unlimited") {
      return {
        type: "unlimited",
        label: "Acceso Ilimitado",
        color: "bg-gradient-to-r from-green-500 to-emerald-600 text-white",
        icon: <Crown className="h-4 w-4" />,
        showCountdown: false
      };
    }

    if (currentClient.subscriptionType === "premium_yearly") {
      return {
        type: "premium",
        label: "Usuario Premium Anual",
        color: "bg-gradient-to-r from-yellow-400 to-orange-500 text-white",
        icon: <Crown className="h-4 w-4" />,
        showCountdown: false
      };
    }

    if (currentClient.subscriptionType === "premium_monthly") {
      return {
        type: "premium",
        label: "Premium Mensual",
        color: "bg-gradient-to-r from-blue-500 to-purple-600 text-white",
        icon: <Crown className="h-4 w-4" />,
        showCountdown: false
      };
    }

    // Para suscripciones explícitamente marcadas como "expired"
    if (currentClient.subscriptionType === "expired") {
      return {
        type: "expired",
        label: "Suscripción Expirada",
        color: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
        icon: <AlertTriangle className="h-4 w-4" />,
        showCountdown: false
      };
    }

    if (currentClient.subscriptionType === "trial" && !timeRemaining.isExpired) {
      return {
        type: "trial",
        label: "Período de Prueba",
        color: "bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200",
        icon: <Clock className="h-4 w-4" />,
        showCountdown: true
      };
    }

    if (currentClient.subscriptionType === "trial" && timeRemaining.isExpired) {
      return {
        type: "expired",
        label: "Suscripción Expirada",
        color: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
        icon: <AlertTriangle className="h-4 w-4" />,
        showCountdown: false
      };
    }

    // No mostrar nada para tipos desconocidos o cuando no hay problemas
    return null;
  };

  const status = getSubscriptionStatus();

  // No mostrar nada si no hay estado o es un tipo que no necesita mostrar
  if (!status) {
    return null;
  }

  // Componente de contacto para suscripciones expiradas
  const ContactDialog = () => (
    <Dialog>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm" className="ml-2">
          <MessageCircle className="h-4 w-4 mr-2" />
          Contactar Ventas
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Renovar Suscripción</DialogTitle>
        </DialogHeader>
        <div className="space-y-4">
          <p className="text-sm text-gray-600 dark:text-gray-400">
            Tu período de prueba ha expirado. Contacta a nuestro equipo de ventas para renovar tu suscripción.
          </p>
          
          {currentClient.salesContactNumber && (
            <div className="flex items-center justify-between p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
              <div className="flex items-center space-x-2">
                <MessageCircle className="h-5 w-5 text-blue-600" />
                <div>
                  <p className="text-sm font-medium">Teléfono de Ventas</p>
                  <p className="text-lg font-bold text-blue-600">{currentClient.salesContactNumber}</p>
                </div>
              </div>
              <Button 
                size="sm" 
                className="bg-blue-600 hover:bg-blue-700"
                onClick={() => {
                  const phoneNumber = currentClient.salesContactNumber?.replace(/\D/g, '') || '';
                  const message = encodeURIComponent('Hola, necesito renovar mi suscripción de StockCel. ¿Podrían ayudarme?');
                  window.open(`https://wa.me/${phoneNumber}?text=${message}`, '_blank');
                }}
              >
                Enviar Mensaje
              </Button>
            </div>
          )}
          
          <div className="flex items-center space-x-2 p-3 bg-green-50 dark:bg-green-900/20 rounded-lg">
            <Mail className="h-5 w-5 text-green-600" />
            <div>
              <p className="text-sm font-medium">Email de Soporte</p>
              <p className="text-lg font-bold text-green-600">ventas@stockcel.com</p>
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );

  return (
    <Card className="bg-white dark:bg-gray-800 shadow-sm">
      <CardContent className="p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <Badge className={status.color}>
              {status.icon}
              <span className="ml-2">{status.label}</span>
            </Badge>
            
            {status.showCountdown && !timeRemaining.isExpired && (
              <div className="text-sm text-gray-600 dark:text-gray-400">
                <span className="font-mono bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">
                  {timeRemaining.days}d {timeRemaining.hours}h {timeRemaining.minutes}m {timeRemaining.seconds}s
                </span>
              </div>
            )}
            
            {status.type === "premium" && (
              <span className="text-xs text-gray-500 bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">
                Acceso completo
              </span>
            )}
          </div>

          {(status.type === "expired" || timeRemaining.isExpired) && (
            <ContactDialog />
          )}
        </div>

        {status.showCountdown && client.trialEndDate && (
          <div className="mt-3 text-xs text-gray-500">
            Prueba hasta: {new Date(client.trialEndDate).toLocaleDateString('es-ES', {
              year: 'numeric',
              month: 'long',
              day: 'numeric',
              hour: '2-digit',
              minute: '2-digit'
            })}
          </div>
        )}
      </CardContent>
    </Card>
  );
}