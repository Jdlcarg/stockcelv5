import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { AuthProvider } from "@/hooks/use-auth";
import { ThemeProvider } from "@/hooks/use-theme";
import ProtectedRoute from "@/components/protected-route";
import Login from "@/pages/login";
import Dashboard from "@/pages/dashboard";
import Products from "@/pages/products";
import Orders from "@/pages/orders";
import CreateOrder from "@/pages/create-order";
import Cash from "@/pages/cash";
import Users from "@/pages/users";
import Vendors from "@/pages/vendors";
import Customers from "@/pages/customers";
import CurrencyConverter from "@/pages/currency-converter";
import Configuration from "@/pages/configuration";
import CashAdvanced from "@/pages/cash-advanced";
import PagosGastos from "@/pages/pagos-gastos";
import Settings from "@/pages/settings";
import StockControl from "@/pages/stock-control-final";
import InvoiceNew from "@/pages/invoice-new";
import FullReset from "@/pages/full-reset";
import Reportes from "@/pages/reportes";
import AdminCreator from "@/pages/admin-creator";
import AdminDashboard from "@/pages/admin-dashboard";
import SystemConfiguration from "@/pages/system-configuration";
import ResellersManagement from "@/pages/resellers-management";
import ResellerDashboard from "@/pages/reseller-dashboard";
import ResellerLogin from "@/pages/reseller-login";
import ResellerAdminPanel from "@/pages/reseller-admin-panel";
import ResellerSettings from "@/pages/reseller-settings";
import AutoSyncMonitor from "@/pages/auto-sync-monitor";
import CashScheduleConfig from "@/pages/cash-schedule-config";

import ChangePassword from "@/pages/change-password";
import ForgotPasswordPage from "@/pages/forgot-password";
import ResetPasswordPage from "@/pages/reset-password";

import NotFound from "@/pages/not-found";

function Router() {
  return (
    <Switch>
      <Route path="/" component={Login} />
      <Route path="/dashboard">
            <ProtectedRoute requiredPermission="dashboard">
              <Dashboard />
            </ProtectedRoute>
          </Route>
      <Route path="/products" component={Products} />
      <Route path="/orders" component={Orders} />
      <Route path="/create-order" component={CreateOrder} />
      <Route path="/cash" component={Cash} />
      <Route path="/users" component={Users} />
      <Route path="/vendors" component={Vendors} />
      <Route path="/customers" component={Customers} />
      <Route path="/currency-converter" component={CurrencyConverter} />
      <Route path="/configuration" component={Configuration} />
      <Route path="/cash-advanced" component={CashAdvanced} />  
      <Route path="/pagos-gastos" component={PagosGastos} />
      <Route path="/reportes" component={Reportes} />
      <Route path="/settings" component={Settings} />
      <Route path="/stock-control" component={StockControl} />
      <Route path="/full-reset" component={FullReset} />
      <Route path="/admin/create" component={AdminCreator} />
      <Route path="/admin/dashboard" component={AdminDashboard} />
      <Route path="/admin/system-configuration" component={SystemConfiguration} />
      <Route path="/admin/resellers-management" component={ResellersManagement} />
      <Route path="/reseller/dashboard" component={ResellerDashboard} />
      <Route path="/reseller-login" component={ResellerLogin} />
      <Route path="/reseller-panel" component={ResellerAdminPanel} />
      <Route path="/reseller-settings" component={ResellerSettings} />
      <Route path="/auto-sync-monitor" component={AutoSyncMonitor} />
      <Route path="/cash-schedule" component={CashScheduleConfig} />

      <Route path="/change-password" component={ChangePassword} />
      <Route path="/forgot-password" component={ForgotPasswordPage} />
      <Route path="/reset-password" component={ResetPasswordPage} />

      <Route path="/invoice/:orderId" component={InvoiceNew} />
      <Route component={NotFound} />
    </Switch>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider defaultTheme="light" storageKey="stockcel-theme">
        <AuthProvider>
          <TooltipProvider>
            <Toaster />
            <Router />
          </TooltipProvider>
        </AuthProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
}

export default App;