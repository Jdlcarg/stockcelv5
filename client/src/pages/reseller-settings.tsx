import { useState, useEffect } from "react";
import { useLocation } from "wouter";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { toast } from "@/hooks/use-toast";
import { 
  ArrowLeft,
  Settings, 
  User, 
  DollarSign, 
  Calendar,
  Save,
  LogOut
} from "lucide-react";

interface ResellerConfig {
  resellerId: number;
  companyName: string;
  contactEmail: string;
  pricePerAccount: string;
  defaultTrialDays: number;
  paymentMethods: string[];
}

export default function ResellerSettings() {
  const [, setLocation] = useLocation();
  const queryClient = useQueryClient();
  const [user, setUser] = useState<any>(null);

  useEffect(() => {
    const storedUser = localStorage.getItem("user");
    if (storedUser) {
      setUser(JSON.parse(storedUser));
    } else {
      setLocation("/reseller-login");
    }
  }, [setLocation]);

  const [config, setConfig] = useState({
    companyName: "",
    contactEmail: "",
    pricePerAccount: "1500.00",
    defaultTrialDays: 30,
    paymentMethods: ["transferencia", "efectivo"]
  });

  // Query para obtener configuración
  const { data: resellerConfig, isLoading } = useQuery<ResellerConfig>({
    queryKey: ["/api/reseller/configuration", user?.id],
    queryFn: async () => {
      const response = await fetch("/api/reseller/configuration", {
        headers: { 
          "x-user-id": user?.id?.toString() || "",
          "Authorization": `Bearer ${localStorage.getItem("authToken")}`
        },
      });
      if (!response.ok) throw new Error("Failed to fetch configuration");
      return response.json();
    },
    enabled: !!user?.id,
  });

  // Actualizar estado local cuando se carga la configuración
  useEffect(() => {
    if (resellerConfig) {
      setConfig({
        companyName: resellerConfig.companyName || "",
        contactEmail: resellerConfig.contactEmail || user?.email || "",
        pricePerAccount: resellerConfig.pricePerAccount || "1500.00",
        defaultTrialDays: resellerConfig.defaultTrialDays || 30,
        paymentMethods: resellerConfig.paymentMethods || ["transferencia", "efectivo"]
      });
    }
  }, [resellerConfig, user?.email]);

  // Mutación para actualizar configuración
  const updateConfigMutation = useMutation({
    mutationFn: async (configData: any) => {
      const response = await fetch("/api/reseller/configuration", {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          "x-user-id": user?.id?.toString() || "",
          "Authorization": `Bearer ${localStorage.getItem("authToken")}`
        },
        body: JSON.stringify(configData)
      });
      
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || "Error al actualizar configuración");
      }
      
      return response.json();
    },
    onSuccess: () => {
      toast({
        title: "Configuración actualizada",
        description: "Los cambios han sido guardados exitosamente.",
      });
      queryClient.invalidateQueries({ queryKey: ["/api/reseller/configuration"] });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Error al actualizar configuración",
        variant: "destructive",
      });
    },
  });

  const handleSave = () => {
    updateConfigMutation.mutate(config);
  };

  const handleLogout = async () => {
    try {
      localStorage.removeItem("user");
      localStorage.removeItem("authToken");
      window.location.href = '/reseller-login';
    } catch (error) {
      console.error('Error durante logout:', error);
      window.location.href = '/reseller-login';
    }
  };

  if (!user) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-900 via-blue-800 to-indigo-900">
      {/* Header */}
      <div className="bg-white/10 backdrop-blur-sm border-b border-white/20">
        <div className="max-w-6xl mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setLocation("/reseller-panel")}
                className="text-white hover:bg-white/10"
              >
                <ArrowLeft className="h-4 w-4 mr-2" />
                Volver al Panel
              </Button>
              <h1 className="text-2xl font-bold text-white">
                Configuración del Revendedor
              </h1>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={handleLogout}
              className="text-white border-white/30 hover:bg-white/10"
            >
              <LogOut className="h-4 w-4 mr-2" />
              Cerrar Sesión
            </Button>
          </div>
        </div>
      </div>

      <div className="max-w-4xl mx-auto p-6">
        {/* Información del Usuario */}
        <Card className="mb-6 bg-white/95 backdrop-blur-sm">
          <CardHeader>
            <CardTitle className="flex items-center">
              <User className="h-5 w-5 mr-2" />
              Información Personal
            </CardTitle>
          </CardHeader>
          <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <Label className="text-sm font-medium">Nombre</Label>
              <p className="text-lg font-semibold text-gray-900">{user?.name}</p>
            </div>
            <div>
              <Label className="text-sm font-medium">Email</Label>
              <p className="text-lg font-semibold text-gray-900">{user?.email}</p>
            </div>
            <div>
              <Label className="text-sm font-medium">Cuota de Cuentas</Label>
              <p className="text-lg font-semibold text-blue-600">{user?.accountsQuota || "N/A"}</p>
            </div>
            <div>
              <Label className="text-sm font-medium">Cuentas Vendidas</Label>
              <p className="text-lg font-semibold text-green-600">{user?.accountsSold || 0}</p>
            </div>
          </CardContent>
        </Card>

        {/* Configuración de Ventas */}
        <Card className="mb-6 bg-white/95 backdrop-blur-sm">
          <CardHeader>
            <CardTitle className="flex items-center">
              <Settings className="h-5 w-5 mr-2" />
              Configuración de Ventas
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            {isLoading ? (
              <div className="flex justify-center items-center py-8">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
              </div>
            ) : (
              <>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="companyName">Nombre de la Empresa</Label>
                    <Input
                      id="companyName"
                      value={config.companyName}
                      onChange={(e) => setConfig(prev => ({ ...prev, companyName: e.target.value }))}
                      placeholder="Mi Empresa de Reventas"
                    />
                  </div>
                  <div>
                    <Label htmlFor="contactEmail">Email de Contacto</Label>
                    <Input
                      id="contactEmail"
                      type="email"
                      value={config.contactEmail}
                      onChange={(e) => setConfig(prev => ({ ...prev, contactEmail: e.target.value }))}
                      placeholder="contacto@empresa.com"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <Label htmlFor="pricePerAccount" className="flex items-center">
                      <DollarSign className="h-4 w-4 mr-1" />
                      Precio por Cuenta (ARS)
                    </Label>
                    <Input
                      id="pricePerAccount"
                      value={config.pricePerAccount}
                      onChange={(e) => setConfig(prev => ({ ...prev, pricePerAccount: e.target.value }))}
                      placeholder="1500.00"
                    />
                  </div>
                  <div>
                    <Label htmlFor="defaultTrialDays" className="flex items-center">
                      <Calendar className="h-4 w-4 mr-1" />
                      Días de Prueba por Defecto
                    </Label>
                    <Input
                      id="defaultTrialDays"
                      type="number"
                      value={config.defaultTrialDays}
                      onChange={(e) => setConfig(prev => ({ ...prev, defaultTrialDays: parseInt(e.target.value) }))}
                      placeholder="30"
                    />
                  </div>
                </div>

                <div>
                  <Label>Métodos de Pago Aceptados</Label>
                  <div className="flex flex-wrap gap-2 mt-2">
                    {config.paymentMethods.map((method) => (
                      <Badge key={method} variant="secondary">
                        {method}
                      </Badge>
                    ))}
                  </div>
                  <p className="text-xs text-gray-500 mt-1">
                    Los métodos de pago se pueden personalizar contactando al soporte
                  </p>
                </div>

                <div className="flex justify-end pt-4">
                  <Button 
                    onClick={handleSave}
                    disabled={updateConfigMutation.isPending}
                    className="bg-blue-600 hover:bg-blue-700"
                  >
                    {updateConfigMutation.isPending ? (
                      <>
                        <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                        Guardando...
                      </>
                    ) : (
                      <>
                        <Save className="h-4 w-4 mr-2" />
                        Guardar Configuración
                      </>
                    )}
                  </Button>
                </div>
              </>
            )}
          </CardContent>
        </Card>

        {/* Información Adicional */}
        <Card className="bg-blue-50 border-blue-200">
          <CardHeader>
            <CardTitle className="text-blue-900">Información Importante</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2 text-sm text-blue-800">
              <li>• Los cambios en la configuración se aplicarán inmediatamente a nuevas ventas</li>
              <li>• Para modificar los métodos de pago, contacte al soporte técnico</li>
              <li>• El precio por cuenta es el que cobrará a sus clientes</li>
              <li>• Los días de prueba se aplicarán automáticamente a nuevos clientes</li>
            </ul>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}