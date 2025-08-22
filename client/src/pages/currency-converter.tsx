import { useState, useEffect } from "react";
import { useLocation } from "wouter";
import { useAuth } from "@/hooks/use-auth";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { RefreshCw, Calculator, ArrowLeftRight } from "lucide-react";
import { toast } from "@/hooks/use-toast";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";

export default function CurrencyConverter() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();
  
  // Exchange rate states
  const [exchangeRates, setExchangeRates] = useState({
    arsToUsd: 1100
  });
  
  // Modal states
  const [showArsToUsdModal, setShowArsToUsdModal] = useState(false);
  
  // Form states for ARS → USD
  const [arsAmount, setArsAmount] = useState("");
  const [usdResult, setUsdResult] = useState("");

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  // Update exchange rate and recalculate
  const updateArsToUsdRate = (newRate: number) => {
    setExchangeRates({ arsToUsd: newRate });
    if (arsAmount) {
      const result = parseFloat(arsAmount) / newRate;
      setUsdResult(result.toFixed(2));
    }
    toast({
      title: "TC Actualizado",
      description: `Tipo de cambio ARS → USD: ${newRate}`,
    });
  };

  if (!isAuthenticated) {
    return null;
  }

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
      
      <div className="flex">
        <Sidebar />
        
        <main className="flex-1 bg-gray-50 dark:bg-gray-900 min-w-0">
          <div className="p-6 space-y-6">
            <div className="flex items-center justify-between">
              <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Configuración TC (Tipos de Cambio)</h1>
            </div>

            {/* TC Configuration Card */}
            <div className="max-w-md mx-auto">
              {/* ARS → USD */}
              <Card className="border-2 border-blue-200 dark:border-blue-700">
                <CardHeader className="pb-4">
                  <CardTitle className="flex items-center gap-2 text-blue-700 dark:text-blue-300">
                    <ArrowLeftRight className="h-5 w-5" />
                    Cotización ARS → USD
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="text-center p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                    <div className="text-2xl font-bold text-blue-700 dark:text-blue-300">
                      TC: {exchangeRates.arsToUsd.toLocaleString()}
                    </div>
                    <p className="text-sm text-blue-600 dark:text-blue-400">
                      1 USD = {exchangeRates.arsToUsd} ARS
                    </p>
                  </div>
                  
                  <Button 
                    onClick={() => setShowArsToUsdModal(true)}
                    className="w-full bg-blue-600 hover:bg-blue-700 text-white"
                  >
                    <Calculator className="h-4 w-4 mr-2" />
                    Calcular & Configurar TC
                  </Button>
                </CardContent>
              </Card>
            </div>

            {/* Usage Instructions */}
            <Card className="border-yellow-200 dark:border-yellow-700">
              <CardContent className="p-6">
                <div className="flex items-start gap-3">
                  <div className="bg-yellow-100 dark:bg-yellow-900/20 p-2 rounded-lg">
                    <Calculator className="h-5 w-5 text-yellow-600 dark:text-yellow-400" />
                  </div>
                  <div>
                    <h3 className="font-semibold text-yellow-800 dark:text-yellow-200 mb-2">
                      Instrucciones de Uso
                    </h3>
                    <ul className="text-sm text-yellow-700 dark:text-yellow-300 space-y-1">
                      <li>• Use los modales para configurar el tipo de cambio actual</li>
                      <li>• Ingrese montos para calcular equivalencias automáticamente</li>
                      <li>• Los TC configurados se aplicarán en todo el sistema</li>
                      <li>• Ideal para cotizaciones diarias con clientes</li>
                    </ul>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* ARS → USD Modal */}
          <Dialog open={showArsToUsdModal} onOpenChange={setShowArsToUsdModal}>
            <DialogContent className="max-w-md">
              <DialogHeader>
                <DialogTitle className="flex items-center gap-2 text-blue-700">
                  <ArrowLeftRight className="h-5 w-5" />
                  Cotización ARS → USD
                </DialogTitle>
              </DialogHeader>
              <div className="space-y-4">
                <div>
                  <Label htmlFor="tc-ars-usd">Configurar Tipo de Cambio</Label>
                  <Input 
                    id="tc-ars-usd"
                    type="number"
                    step="0.01"
                    placeholder="Ej: 1100"
                    onKeyPress={(e) => {
                      if (e.key === 'Enter') {
                        const newRate = parseFloat((e.target as HTMLInputElement).value);
                        if (newRate > 0) {
                          updateArsToUsdRate(newRate);
                        }
                      }
                    }}
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Presione Enter para actualizar el TC
                  </p>
                </div>

                <div className="border-t pt-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <Label htmlFor="ars-amount">Monto ARS</Label>
                      <Input 
                        id="ars-amount"
                        type="number"
                        step="0.01"
                        value={arsAmount}
                        onChange={(e) => {
                          setArsAmount(e.target.value);
                          if (e.target.value) {
                            const result = parseFloat(e.target.value) / exchangeRates.arsToUsd;
                            setUsdResult(result.toFixed(2));
                          } else {
                            setUsdResult("");
                          }
                        }}
                        placeholder="0.00"
                      />
                    </div>
                    <div>
                      <Label htmlFor="usd-result">Equivalente USD</Label>
                      <Input 
                        id="usd-result"
                        type="number"
                        value={usdResult}
                        readOnly
                        className="bg-gray-100 dark:bg-gray-700"
                        placeholder="0.00"
                      />
                    </div>
                  </div>
                  <div className="mt-3 text-center text-sm text-gray-600 dark:text-gray-400">
                    TC actual: 1 USD = {exchangeRates.arsToUsd.toLocaleString()} ARS
                  </div>
                </div>
              </div>
            </DialogContent>
          </Dialog>


        </main>
      </div>
    </div>
  );
}