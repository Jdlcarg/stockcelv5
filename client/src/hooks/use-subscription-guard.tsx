import { useEffect, useState } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { AlertTriangle, Phone, Mail, Crown, MessageCircle } from "lucide-react";

interface UseSubscriptionGuardProps {
  client: {
    subscriptionType: string;
    trialEndDate?: string;
    salesContactNumber?: string;
  };
  user: {
    role: string;
  };
}

export function useSubscriptionGuard({ client, user }: UseSubscriptionGuardProps) {
  const [isBlocked, setIsBlocked] = useState(false);
  const [showDialog, setShowDialog] = useState(false);

  const checkSubscriptionStatus = () => {
    if (user.role === "superuser") return false; // SuperUser nunca se bloquea
    
    // Usuarios con acceso ilimitado nunca se bloquean
    if (client.subscriptionType === "unlimited") return false;
    
    // Usuarios premium nunca se bloquean
    if (client.subscriptionType === "premium_yearly" || client.subscriptionType === "premium_monthly") return false;

    // Verificar si la suscripción de prueba ha expirado
    if (client.subscriptionType === "trial" && client.trialEndDate) {
      const now = new Date();
      const endDate = new Date(client.trialEndDate);
      if (now > endDate) {
        return true; // Expirado
      }
    }

    if (client.subscriptionType === "expired") {
      return true;
    }

    return false;
  };

  useEffect(() => {
    const blocked = checkSubscriptionStatus();
    setIsBlocked(blocked);
    if (blocked) {
      setShowDialog(true);
    }
  }, [client.subscriptionType, client.trialEndDate]);

  const SubscriptionBlockedDialog = () => (
    <Dialog open={showDialog} onOpenChange={() => {}}>
      <DialogContent className="max-w-md" onPointerDownOutside={(e) => e.preventDefault()}>
        <DialogHeader>
          <DialogTitle className="flex items-center space-x-2 text-red-600">
            <AlertTriangle className="h-6 w-6" />
            <span>Suscripción Expirada</span>
          </DialogTitle>
        </DialogHeader>
        
        <div className="space-y-4">
          <div className="bg-red-50 dark:bg-red-900/20 p-4 rounded-lg">
            <p className="text-sm text-red-800 dark:text-red-200">
              Tu período de prueba ha expirado. Para continuar usando StockCel, necesitas renovar tu suscripción.
            </p>
          </div>

          <div className="space-y-3">
            <h4 className="font-medium text-gray-900 dark:text-white">
              Planes disponibles:
            </h4>
            
            <div className="grid gap-2">
              <div className="flex items-center justify-between p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                <div className="flex items-center space-x-2">
                  <Crown className="h-4 w-4 text-blue-600" />
                  <span className="text-sm font-medium">Premium Mensual</span>
                </div>
                <span className="text-lg font-bold text-blue-600">$29.99/mes</span>
              </div>
              
              <div className="flex items-center justify-between p-3 bg-gradient-to-r from-yellow-50 to-orange-50 dark:from-yellow-900/20 dark:to-orange-900/20 rounded-lg border-2 border-yellow-200 dark:border-yellow-700">
                <div className="flex items-center space-x-2">
                  <Crown className="h-4 w-4 text-yellow-600" />
                  <span className="text-sm font-medium">Premium Anual</span>
                  <span className="text-xs bg-green-100 text-green-800 px-2 py-1 rounded">Más Popular</span>
                </div>
                <div className="text-right">
                  <span className="text-lg font-bold text-yellow-600">$299.99/año</span>
                  <div className="text-xs text-gray-500">2 meses gratis</div>
                </div>
              </div>
            </div>
          </div>

          <div className="space-y-3">
            <h4 className="font-medium text-gray-900 dark:text-white">
              Contacta a ventas:
            </h4>
            
            {client.salesContactNumber && (
              <div className="flex items-center justify-between p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                <div className="flex items-center space-x-2">
                  <MessageCircle className="h-5 w-5 text-blue-600" />
                  <div>
                    <p className="text-sm font-medium">Teléfono de Ventas</p>
                    <p className="text-lg font-bold text-blue-600">{client.salesContactNumber}</p>
                  </div>
                </div>
                <Button 
                  size="sm" 
                  className="bg-blue-600 hover:bg-blue-700"
                  onClick={() => {
                    const phoneNumber = client.salesContactNumber?.replace(/\D/g, '') || '';
                    const message = encodeURIComponent('Hola, necesito renovar mi suscripción de StockCel. ¿Podrían ayudarme?');
                    window.open(`https://wa.me/${phoneNumber}?text=${message}`, '_blank');
                  }}
                >
                  Enviar Mensaje
                </Button>
              </div>
            )}
            
            <div className="flex items-center space-x-3 p-3 bg-green-50 dark:bg-green-900/20 rounded-lg">
              <Mail className="h-5 w-5 text-green-600" />
              <div>
                <p className="text-sm font-medium">Email de Soporte</p>
                <p className="text-lg font-bold text-green-600">ventas@stockcel.com</p>
              </div>
              <Button 
                size="sm" 
                onClick={() => window.open('mailto:ventas@stockcel.com')}
                className="ml-auto"
              >
                Escribir
              </Button>
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );

  return {
    isBlocked,
    SubscriptionBlockedDialog
  };
}