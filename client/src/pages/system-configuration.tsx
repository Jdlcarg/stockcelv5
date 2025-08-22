import { useState } from "react";
import { useLocation } from "wouter";
import { useAuth } from "@/hooks/use-auth";
import { useToast } from "@/hooks/use-toast";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Switch } from "@/components/ui/switch";
import { Settings, DollarSign, Phone, Mail, Crown, AlertTriangle, MessageCircle } from "lucide-react";

export default function SystemConfiguration() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();

  // Redirect if not superuser
  if (!isAuthenticated || user?.role !== "superuser") {
    setLocation("/");
    return null;
  }

  // System configuration state
  const [config, setConfig] = useState({
    // Planes de suscripción
    premiumMonthlyPrice: "$29.99/mes",
    premiumYearlyPrice: "$299.99/año",
    premiumYearlyDiscount: "2 meses gratis",
    premiumYearlyPopular: true,
    
    // Información de contacto
    salesPhone: "1170627214",
    salesEmail: "ventas@stockcel.com",
    supportEmail: "soporte@stockcel.com",
    
    // Mensajes
    expiredTitle: "Suscripción Expirada",
    expiredMessage: "Tu período de prueba ha expirado. Para continuar usando StockCel, necesitas renovar tu suscripción.",
    plansTitle: "Planes disponibles:",
    contactTitle: "Contacta a ventas:"
  });

  // Get current system configuration
  const { data: systemConfig, isLoading } = useQuery({
    queryKey: ["/api/system-configuration"],
    queryFn: async () => {
      const response = await apiRequest("GET", "/api/system-configuration");
      if (response.ok) {
        const data = await response.json();
        setConfig(data);
        return data;
      }
      return null;
    }
  });

  // Update system configuration
  const updateConfigMutation = useMutation({
    mutationFn: async (updates: Partial<typeof config>) => {
      const response = await apiRequest("PUT", "/api/system-configuration", updates);
      if (!response.ok) {
        throw new Error("Error updating system configuration");
      }
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/system-configuration"] });
      toast({
        title: "Configuración actualizada",
        description: "Los cambios han sido guardados correctamente.",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Error al actualizar configuración",
        variant: "destructive",
      });
    }
  });

  const handleInputChange = (field: string, value: string | boolean) => {
    setConfig(prev => ({ ...prev, [field]: value }));
  };

  const handleSave = () => {
    updateConfigMutation.mutate(config);
  };

  const handlePreview = () => {
    // Simular un modal de suscripción expirada con la configuración actual
    const modalContent = `
      <div style="position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.5); z-index: 9999; display: flex; align-items: center; justify-content: center;">
        <div style="background: white; padding: 24px; border-radius: 8px; max-width: 500px; width: 90%;">
          <h2 style="color: #ef4444; font-size: 24px; font-weight: bold; margin-bottom: 16px;">${config.expiredTitle}</h2>
          <p style="color: #6b7280; margin-bottom: 24px;">${config.expiredMessage}</p>
          
          <h3 style="font-weight: 600; margin-bottom: 16px;">${config.plansTitle}</h3>
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px;">
            <div style="border: 1px solid #e5e7eb; padding: 16px; border-radius: 6px;">
              <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px;">
                <span style="font-weight: 500;">Premium Mensual</span>
                <span style="background: #f3f4f6; padding: 4px 8px; border-radius: 4px; font-size: 14px;">${config.premiumMonthlyPrice}</span>
              </div>
            </div>
            <div style="border: 1px solid #e5e7eb; padding: 16px; border-radius: 6px;">
              <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px;">
                <span style="font-weight: 500;">Premium Anual</span>
                <div style="display: flex; align-items: center; gap: 8px;">
                  ${config.premiumYearlyPopular ? '<span style="background: linear-gradient(45deg, #fbbf24, #f59e0b); color: white; padding: 2px 6px; border-radius: 4px; font-size: 12px;">Más Popular</span>' : ''}
                  <span style="background: #f3f4f6; padding: 4px 8px; border-radius: 4px; font-size: 14px;">${config.premiumYearlyPrice}</span>
                </div>
              </div>
              <p style="font-size: 14px; color: #6b7280;">${config.premiumYearlyDiscount}</p>
            </div>
          </div>
          
          <h3 style="font-weight: 600; margin-bottom: 16px;">${config.contactTitle}</h3>
          <div style="display: flex; gap: 16px; margin-bottom: 24px;">
            <div style="display: flex; align-items: center; gap: 8px; padding: 12px; background: #dbeafe; border-radius: 6px;">
              <span style="color: #2563eb;">📱</span>
              <div>
                <p style="font-size: 14px; font-weight: 500;">WhatsApp</p>
                <p style="font-size: 16px; font-weight: bold; color: #2563eb;">${config.salesPhone}</p>
              </div>
            </div>
            <div style="display: flex; align-items: center; gap: 8px; padding: 12px; background: #dcfce7; border-radius: 6px;">
              <span style="color: #16a34a;">✉️</span>
              <div>
                <p style="font-size: 14px; font-weight: 500;">Email</p>
                <p style="font-size: 16px; font-weight: bold; color: #16a34a;">${config.salesEmail}</p>
              </div>
            </div>
          </div>
          
          <button onclick="this.parentElement.parentElement.remove()" style="background: #6b7280; color: white; padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer;">
            Cerrar Vista Previa
          </button>
        </div>
      </div>
    `;
    
    const previewModal = document.createElement('div');
    previewModal.innerHTML = modalContent;
    document.body.appendChild(previewModal);
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-background flex flex-col">
        <Header 
          onMobileNavToggle={() => setMobileNavOpen(!mobileNavOpen)}
          isMobileNavOpen={mobileNavOpen}
          showMenuButton={true}
        />
        <div className="flex flex-1 overflow-hidden">
          <Sidebar 
            isOpen={!mobileNavOpen} 
            userRole={user?.role || ''} 
          />
          <main className="flex-1 overflow-auto bg-background">
            <div className="container mx-auto px-4 py-6">
              <div className="flex items-center justify-center h-96">
                <div className="text-center">
                  <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
                  <p className="mt-4 text-gray-600 dark:text-gray-400">Cargando configuración...</p>
                </div>
              </div>
            </div>
          </main>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <Header 
        onMobileNavToggle={() => setMobileNavOpen(!mobileNavOpen)}
        isMobileNavOpen={mobileNavOpen}
        showMenuButton={true}
      />

      <div className="flex flex-1 overflow-hidden">
        <Sidebar 
          isOpen={!mobileNavOpen} 
          userRole={user?.role || ''} 
        />
        <MobileNav 
          isOpen={mobileNavOpen} 
          onClose={() => setMobileNavOpen(false)}
          userRole={user?.role || ''}
        />

        <main className="flex-1 overflow-auto bg-background">
          <div className="container mx-auto px-4 py-6">
            <div className="mb-6">
              <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                Configuración del Sistema
              </h1>
              <p className="text-gray-600 dark:text-gray-400">
                Gestiona los planes de suscripción, precios y mensajes del sistema
              </p>
            </div>

            <div className="mb-6 flex gap-4">
              <Button onClick={handleSave} disabled={updateConfigMutation.isPending}>
                {updateConfigMutation.isPending ? "Guardando..." : "Guardar Cambios"}
              </Button>
              <Button variant="outline" onClick={handlePreview}>
                Vista Previa
              </Button>
            </div>

            <Tabs defaultValue="plans" className="space-y-6">
              <TabsList className="grid w-full grid-cols-3">
                <TabsTrigger value="plans" className="flex items-center gap-2">
                  <Crown className="h-4 w-4" />
                  Planes de Suscripción
                </TabsTrigger>
                <TabsTrigger value="contact" className="flex items-center gap-2">
                  <Phone className="h-4 w-4" />
                  Información de Contacto
                </TabsTrigger>
                <TabsTrigger value="messages" className="flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4" />
                  Mensajes del Sistema
                </TabsTrigger>
              </TabsList>

              <TabsContent value="plans" className="space-y-6">
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <DollarSign className="h-5 w-5" />
                      Planes de Suscripción
                    </CardTitle>
                    <CardDescription>
                      Configura los precios y características de los planes
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-6">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                      <div className="space-y-4">
                        <div>
                          <Label htmlFor="premium-monthly">Plan Premium Mensual</Label>
                          <Input
                            id="premium-monthly"
                            value={config.premiumMonthlyPrice}
                            onChange={(e) => handleInputChange("premiumMonthlyPrice", e.target.value)}
                            placeholder="$29.99/mes"
                          />
                        </div>
                      </div>

                      <div className="space-y-4">
                        <div>
                          <Label htmlFor="premium-yearly">Plan Premium Anual</Label>
                          <Input
                            id="premium-yearly"
                            value={config.premiumYearlyPrice}
                            onChange={(e) => handleInputChange("premiumYearlyPrice", e.target.value)}
                            placeholder="$299.99/año"
                          />
                        </div>
                        <div>
                          <Label htmlFor="yearly-discount">Descuento Anual</Label>
                          <Input
                            id="yearly-discount"
                            value={config.premiumYearlyDiscount}
                            onChange={(e) => handleInputChange("premiumYearlyDiscount", e.target.value)}
                            placeholder="2 meses gratis"
                          />
                        </div>
                        <div className="flex items-center space-x-2">
                          <Switch
                            id="yearly-popular"
                            checked={config.premiumYearlyPopular}
                            onCheckedChange={(checked) => handleInputChange("premiumYearlyPopular", checked)}
                          />
                          <Label htmlFor="yearly-popular">Marcar como "Más Popular"</Label>
                        </div>
                      </div>
                    </div>

                    <Separator />

                    <div className="bg-gray-50 dark:bg-gray-800 p-4 rounded-lg">
                      <h4 className="font-medium mb-3">Vista Previa de Planes:</h4>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div className="bg-white dark:bg-gray-700 p-4 rounded-lg border">
                          <div className="flex items-center justify-between mb-2">
                            <span className="font-medium">Premium Mensual</span>
                            <Badge variant="secondary">
                              <Crown className="h-3 w-3 mr-1" />
                              {config.premiumMonthlyPrice}
                            </Badge>
                          </div>
                        </div>
                        <div className="bg-white dark:bg-gray-700 p-4 rounded-lg border">
                          <div className="flex items-center justify-between mb-2">
                            <span className="font-medium">Premium Anual</span>
                            <div className="flex items-center gap-2">
                              {config.premiumYearlyPopular && (
                                <Badge className="bg-gradient-to-r from-yellow-400 to-orange-500 text-white text-xs">
                                  Más Popular
                                </Badge>
                              )}
                              <Badge variant="secondary">
                                <Crown className="h-3 w-3 mr-1" />
                                {config.premiumYearlyPrice}
                              </Badge>
                            </div>
                          </div>
                          <p className="text-sm text-gray-600 dark:text-gray-400">{config.premiumYearlyDiscount}</p>
                        </div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>

              <TabsContent value="contact" className="space-y-6">
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <Phone className="h-5 w-5" />
                      Información de Contacto
                    </CardTitle>
                    <CardDescription>
                      Configura los datos de contacto que verán los clientes
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-6">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                      <div className="space-y-4">
                        <div>
                          <Label htmlFor="sales-phone">Teléfono de Ventas</Label>
                          <Input
                            id="sales-phone"
                            value={config.salesPhone}
                            onChange={(e) => handleInputChange("salesPhone", e.target.value)}
                            placeholder="1170627214"
                          />
                        </div>
                        <div>
                          <Label htmlFor="sales-email">Email de Ventas</Label>
                          <Input
                            id="sales-email"
                            type="email"
                            value={config.salesEmail}
                            onChange={(e) => handleInputChange("salesEmail", e.target.value)}
                            placeholder="ventas@stockcel.com"
                          />
                        </div>
                      </div>

                      <div className="space-y-4">
                        <div>
                          <Label htmlFor="support-email">Email de Soporte</Label>
                          <Input
                            id="support-email"
                            type="email"
                            value={config.supportEmail}
                            onChange={(e) => handleInputChange("supportEmail", e.target.value)}
                            placeholder="soporte@stockcel.com"
                          />
                        </div>
                      </div>
                    </div>

                    <Separator />

                    <div className="bg-gray-50 dark:bg-gray-800 p-4 rounded-lg">
                      <h4 className="font-medium mb-3">Vista Previa de Contacto:</h4>
                      <div className="space-y-3">
                        <div className="flex items-center space-x-3 p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                          <MessageCircle className="h-5 w-5 text-blue-600" />
                          <div>
                            <p className="text-sm font-medium">WhatsApp de Ventas</p>
                            <a 
                              href={`https://wa.me/${config.salesPhone.replace(/\D/g, '')}`}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="text-lg font-bold text-blue-600 hover:text-blue-800 transition-colors cursor-pointer"
                            >
                              {config.salesPhone}
                            </a>
                          </div>
                        </div>
                        <div className="flex items-center space-x-3 p-3 bg-green-50 dark:bg-green-900/20 rounded-lg">
                          <Mail className="h-5 w-5 text-green-600" />
                          <div>
                            <p className="text-sm font-medium">Email de Soporte</p>
                            <p className="text-lg font-bold text-green-600">{config.salesEmail}</p>
                          </div>
                        </div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>

              <TabsContent value="messages" className="space-y-6">
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <AlertTriangle className="h-5 w-5" />
                      Mensajes del Sistema
                    </CardTitle>
                    <CardDescription>
                      Personaliza los mensajes que ven los usuarios
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-6">
                    <div className="space-y-4">
                      <div>
                        <Label htmlFor="expired-title">Título de Suscripción Expirada</Label>
                        <Input
                          id="expired-title"
                          value={config.expiredTitle}
                          onChange={(e) => handleInputChange("expiredTitle", e.target.value)}
                          placeholder="Suscripción Expirada"
                        />
                      </div>
                      <div>
                        <Label htmlFor="expired-message">Mensaje de Suscripción Expirada</Label>
                        <Textarea
                          id="expired-message"
                          value={config.expiredMessage}
                          onChange={(e) => handleInputChange("expiredMessage", e.target.value)}
                          placeholder="Tu período de prueba ha expirado..."
                          rows={3}
                        />
                      </div>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                          <Label htmlFor="plans-title">Título de Planes</Label>
                          <Input
                            id="plans-title"
                            value={config.plansTitle}
                            onChange={(e) => handleInputChange("plansTitle", e.target.value)}
                            placeholder="Planes disponibles:"
                          />
                        </div>
                        <div>
                          <Label htmlFor="contact-title">Título de Contacto</Label>
                          <Input
                            id="contact-title"
                            value={config.contactTitle}
                            onChange={(e) => handleInputChange("contactTitle", e.target.value)}
                            placeholder="Contacta a ventas:"
                          />
                        </div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>
            </Tabs>
          </div>
        </main>
      </div>
    </div>
  );
}