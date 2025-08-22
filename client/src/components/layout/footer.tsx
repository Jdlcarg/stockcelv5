import { useQuery } from "@tanstack/react-query";
import { useAuth } from "@/lib/auth";

export default function Footer() {
  const { user } = useAuth();

  // Get company configuration
  const { data: companyConfig } = useQuery({
    queryKey: ['/api/company-configuration', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/company-configuration?clientId=${user?.clientId}`);
      if (!response.ok) return null;
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  const currentYear = new Date().getFullYear();
  const companyName = companyConfig?.companyName || "Tu Empresa";

  return (
    <footer className="bg-white dark:bg-gray-900 border-t border-gray-200 dark:border-gray-700 mt-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        <div className="flex flex-col md:flex-row justify-between items-center space-y-4 md:space-y-0">
          <div className="text-sm text-gray-600 dark:text-gray-400">
            © {currentYear} {companyName}. Todos los derechos reservados.
          </div>
          <div className="text-sm text-gray-500 dark:text-gray-500">
            Desarrollado por{" "}
            <a 
              href="https://www.softwarepar.com" 
              target="_blank" 
              rel="noopener noreferrer"
              className="text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 font-medium transition-colors"
            >
              SoftwarePar
            </a>
          </div>
        </div>
      </div>
    </footer>
  );
}